#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 30 15:44:02 2019

@author: eddiecohen
"""

from jinja2 import Environment, FileSystemLoader, Template
from operator import itemgetter
import random
import boto3
import sys
import csv
import os
import glob


def main(credsfile, inputfile, id_file_name, link_file_name, tracking_file_name):
  '''Function to publish HITs for Utterance Collection on MTurk
  Arg inputfile: this input file is the data that you want to use for your HIT. For example for an utterance collection task, this file should contains conversations
  separated by new lines, and turns within each conversation are separated by </s>
  # Arg 1 should be the text file containing the conversations with turns separated by </s>, Arg 2 is desired output ID file name, 
  # Arg 3 is desired output link file name, Arg 4 is desired outpout tracking file name'''

  # Actual HIT creation below 


  ''' Uses the templating engine jina. I made a template based off the Amazon MTurk given template for Collection Utterance and our design. 
  Template folder will be uploaded to github. 
  questions.xml is the xml form that publishes a HIT as provided by Amazon 
  at https://blog.mturk.com/tutorial-a-beginners-guide-to-crowdsourcing-ml-training-data-with-python-and-mturk-d8df4bdf2977
  I updated the basic template to use out HTML stlye, which is found in another template HIT.html
  Plan for future is to use jinja to refactor and take out the actual text of HIT.html '''

  
  mturk = connect_to_MTurk(credsfile, False)

  fileloader = FileSystemLoader('templates')      # Accesses the directory 'templates' in the same classpath as this code file. 'templates' contains files for HTML/XML templates
  env = Environment(loader=fileloader)            # Establishes the environment to load a specific file from the templates diretory
  template = env.get_template("quality-instructions.html")    # 'questions.xml' is the basic template which will be filled in. Here we access it, and we fill it when we call render
  
  
  HITlinks = [] # keeps track of HIT links for analysis purposes
  HITIDs = [] # keeps track of HIT ids for analysis purposes   


  tracking_dict_keys =  ["HITID", "prompt1", "prompt2", "prompt3", "prompt4", "prompt5", "qc1", "qc2", "qc3", "qc4", "qc5"] # these keys will be used to map HITID -> conversations. keys for tracking these HITs
  # fill this in based on which HIT is being done 
  tracking_dict_list = []

  #all_data = BERTSWAG(confusingfile, wrongfile)
  all_data = response_eval(inputfile,link_file_name)
  data_points = len(all_data)
  for i in range(0,data_points,5):   
    data = all_data[i:i+5]
    output = template.render(prompts=data)

    with open('templates/task.html','w') as f:
      print(output,file=f)

    task_template = env.get_template('task.xml')

    output = task_template.render()

    with open('task.xml', 'w') as f:
      print(output,file=f)
    task = open(file='task.xml',mode='r').read()
    new_hit = mturk.create_hit(
        Title = 'Evaluate chatbots part 10',
        Description = 'For 5 prompt judge whether different programs responses are appropriate',
        Keywords = 'chatbots, dialogue agents, evaluation, dialog, conversation',
        Reward = '0.35',
        MaxAssignments = 3,
        LifetimeInSeconds = 648000,
        AssignmentDurationInSeconds = 5400,
        AutoApprovalDelayInSeconds = 259200,
        QualificationRequirements = [
          {
            "QualificationTypeId":"00000000000000000040",
            "Comparator":"GreaterThan",
            "IntegerValues":[500]
          },
          {
            "QualificationTypeId":"00000000000000000071",
            "Comparator":"EqualTo",
            "LocaleValues":[{"Country":"US"}]
          },
          {
            "QualificationTypeId":"000000000000000000L0",
            "Comparator":"GreaterThan",
            "IntegerValues":[94]
          }
        ],
        Question = task
    )

    print("A new HIT has been created. You can preview it here:")
    print("https://workersandbox.mturk.com/mturk/preview?groupId=" + new_hit['HIT']['HITGroupId'])
    print("HITID = " + new_hit['HIT']['HITId'] + " (Use to Get Results)")

    HITlinks.append(new_hit['HIT']['HITGroupId'])
    HITIDs.append(new_hit['HIT']['HITId'])
    tracking_dict = {"HITID": new_hit['HIT']['HITId'], "prompt1": data[0]["prompt"],"prompt2": data[1]["prompt"],"prompt3": data[0]["prompt"],"prompt4": data[3]["prompt"],"prompt5": data[4]["prompt"],
    "qc1":data[0]["qc"], "qc2":data[1]["qc"], "qc3":data[2]["qc"],"qc4":data[3]["qc"],"qc5":data[4]["qc"]}                                                                # fill this in based on the HIT being done, the format is found in the function for the HIT
    tracking_dict_list.append(tracking_dict)                        # adds the dictionary to our trackng dictonary list 


  writetxtfile(id_file_name,HITIDs)
  #writetxtfile(link_file_name,HITlinks)
  writecsvfile(tracking_file_name,tracking_dict_list,tracking_dict_keys)

def response_eval(prompts_file,responses_path):
  prompts = []
  with open(prompts_file, 'r') as input:
    for line in input:
      prompts.append(line)
  all_data = [{} for i in range(len(prompts))]
  all_responses = {}
  paths = glob.glob(responses_path+"/*.txt")
  for path in paths:
    bot_name = path.split("/")[1][:-4]
    all_responses[bot_name] = []
    with open(path, 'r') as input:
      for line in input:
        all_responses[bot_name].append(line) 
  for index,prompt in enumerate(prompts):
    all_data[index] = {"prompt":prompt, "models": [], "qc": -1}
    for response in list(all_responses.keys()):
      all_data[index]["models"].append({"response":all_responses[response][index],"bot":response})
    qc = random.randint(1,5)
    all_data[index]["models"].append({"response": "Enter {} for this one, it is quality control".format(qc), "bot":"quality_control"})
    random.shuffle(all_data[index]["models"])
    all_data[index]["qc"] = qc

  return all_data



def ChatEval(conversations):
  allConversations = [] # list of lists of conversations. Each conversation is a list of the turns.
  responders = [] # list of who the worker is acing as as the responder for each conversation. 
  #tracking_dict_keys = ['HITId', 'convo_1', 'convo_2', 'convo_3', 'convo_4', 'convo_5', 'convo_6', 'convo_7', 'convo_8', 'convo_9', 'convo_10']
  #tracking_dict = {'HITID':new_hit['HIT']['HITId'], 'convo_1':allConversations[i], 'convo_2':allConversations[i+1], 'convo_3':allConversations[i+2], 'convo_4':allConversations[i+3], 'convo_5':allConversations[i+4],
  #'convo_6':allConversations[i+5], 'convo_7':allConversations[i+6], 'convo_8':allConversations[i+7], 'convo_9':allConversations[i+8], 'convo_10':allConversations[i+9]}      # creates the tracking dictionary for the HIT assignment
    

  # # '''First formatting block is for the conversations as they were formatted on ChatEval'''
  
  with open(conversations,'r') as input:      
    for line in input:                      # this code formats the conversations into the style we want for the HIT
       dialog = line.split("</s>")
       for i in range(len(dialog)):
         if i % 2 == 0:                            
           dialog[i] = "A: {}".format(dialog[i])
         else:
           dialog[i] = "B: {}".format(dialog[i])    



       responder = ""

       if len(dialog) % 2 == 0:     # if there are #turn mod 2 = 0 turns in the convo, the worker is responding as person A
         responder = "A"
       else:
         responder = "B"            # otherwise the worker is responding as person B

       allConversations.append(dialog)
       responders.append(responder)
  print (allConversations)
  
  # # '''Second formatting block is for the conversations as they have been formatted after being scraped from ESL sites'''
  '''
  with open(conversations, 'r') as input:
     for line in input:
       dialog = line.split("<br />")

       allConversations.append(dialog[0:3])
       responders.append("B")
  '''
  return allConversations, responders
def DBDC(inputfile):

    all_convos = readfiles([inputfile])
    convo_data = readfiles(['DBDCtrack.csv','DBDC240track.csv'])
    
    bot_ids = [data['bot\n'] for data in convo_data]
    conversations = [conversation for conversation in all_convos if not (conversation['dialogid'] in bot_ids)]

    allConversations = []
    for row in conversations:
        conversation = []
        bot = row["dialogid"]
        for i in range(1,12):
          if i==11:
            conversation.append({"bot":bot,"content":row["content{}".format(i)], "annotationid":row["annotationid{}\n".format(i)]})
          else:
            conversation.append({"bot":bot,"content":row["content{}".format(i)], "annotationid":row["annotationid{}".format(i)]}) 
        allConversations.append(conversation)
    return allConversations

def ALEXA(inputfile):
    
    all_convos = []
    with open(inputfile, 'r') as input:
      for row in input:
        all_convos.append(row)
    allConversations = []
    conversation = []
    cnt = 1
    current_convo = ""
    for i in range(len(all_convos)):
      if cnt==1:
        current_convo = all_convos[i]+all_convos[i+1]+all_convos[i+2]
        conversation.append({"bot":len(allConversations), "content":current_convo, "annotationid":cnt})
        cnt+=1
      elif all_convos[i]!="<br>\n":
        if all_convos[i][0:5]=="<br>S":
          if cnt>2:
            current_convo = current_convo+all_convos[i-1]+all_convos[i]
            conversation.append({"bot":len(allConversations), "content":current_convo, "annotationid":cnt-1})
            cnt+=1
          else:
            cnt+=1
      else:
        current_convo = '"'
        cnt = 1
        allConversations.append(conversation)
        conversation = []
    return allConversations


def BERTSWAG(confusingfile, wrongfile):

    # ['HITID', 'video_id1', 'start1', 'video_id2', 'start2', 'video_id3','start3', 'video_id4','start4', 'video_idqc','qualcontrol']

    confusing = readfiles([confusingfile])
    wrong = readfiles([wrongfile])[1:]
    print(len(confusing))
    random.shuffle(confusing)
    random.shuffle(wrong)

    data = []
    for i in range(0,10476,4):    # this loop gets the first 536 starts 
        HIT_data = []
        for j in range(4):
            sentence_data = confusing[i+j]
            sentence_dict = {}
            sentence_dict["name"] = "sentence{}".format(j)
            sentence_dict["beginning"] = sentence_data[" start"]
            endings = [{"name":"confusion_end4","end":sentence_data["confusion_end4\n"]}]
            endings.extend([{"name":"confusion_end{}".format(k),"end":sentence_data[" confusion_end{}".format(k)]} for k in range(4)])
            random.shuffle(endings)
            sentence_dict["endings"] = endings
            sentence_dict["video_id"] = sentence_data["video_id"]
            HIT_data.append(sentence_dict)
        qualcontrol_dict = {}
        qualcontrol_data = wrong[int((i+1)/11)]
        qualcontrol_dict["beginning"] = qualcontrol_data[" start"]
        qualcontrol_dict["video_id"] = qualcontrol_data["video_id"]
        endings = [{"name":"true_end","end":qualcontrol_data[" true_end"]}]
        endings.extend([{"name":"wrong_end{}".format(k),"end":qualcontrol_data[" wrong_end{}".format(k)]} for k in range(2)])
        endings.append({"name":"wrong_end2","end":qualcontrol_data[" wrong_end2\n"]})
        random.shuffle(endings)
        qualcontrol_dict["endings"] = endings
        qualcontrol_dict["name"] = "quality_control"
        HIT_data.append(qualcontrol_dict)
        data.append(HIT_data)

    for i in range(10476,len(confusing),2): # this loop gets the final two starts
      HIT_data = []
      for j in range(2):
        sentence_data = confusing[i+j]
        sentence_dict = {}
        sentence_dict["name"] = "sentence{}".format(j)
        sentence_dict["beginning"] = sentence_data[" start"]
        sentence_dict["video_id"] = sentence_data["video_id"]
        endings = [{"name":"confusion_end4","end":sentence_data["confusion_end4\n"]}]
        endings.extend([{"name":"confusion_end{}".format(k),"end":sentence_data[" confusion_end{}".format(k)]} for k in range(4)])
        random.shuffle(endings)
        sentence_dict["endings"] = endings
        HIT_data.append(sentence_dict)
      qualcontrol_dict = {}
      qualcontrol_data = wrong[int((i+1)/11)]
      qualcontrol_dict["video_id"] = qualcontrol_data["video_id"]
      qualcontrol_dict["beginning"] = qualcontrol_data[" start"]
      endings = [{"name":"true_end","end":qualcontrol_data[" true_end"]}]
      endings.extend([{"name":"wrong_end{}".format(k),"end":qualcontrol_data[" wrong_end{}".format(k)]} for k in range(2)])
      endings.append({"name":"wrong_end2","end":qualcontrol_data[" wrong_end2\n"]})
      random.shuffle(endings)
      qualcontrol_dict["endings"] = endings
      qualcontrol_dict["name"] = "quality_control"
      HIT_data.append(qualcontrol_dict)
      data.append(HIT_data) 

    return data

def connect_to_MTurk(credsfile, sandbox):
  '''Method to create an instance of an MTurk requester client.
  Arg1: credentials file containing the access key and secret access key of the requester separated by a comma on the second line of the file
  Arg2: Boolean for whether this client should connect to sandbox (True) or connect to the actual marketplace (False)'''
  with open(credsfile, 'r') as creds_input:
    for i,line in enumerate(creds_input):
      if i==1:
        comma_split = line.split(",")
        access_key_id = comma_split[0].strip()
        secret_access_key = comma_split[1].strip()


  MTURK_SANDBOX = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
  if sandbox:
    mturk = boto3.client('mturk',
       aws_access_key_id = access_key_id,
       aws_secret_access_key = secret_access_key,
       region_name='us-east-1',
       endpoint_url = MTURK_SANDBOX  # to access MTurk marketplace leave out endpoint_url completely
    )
  else:
    mturk = boto3.client('mturk',
       aws_access_key_id = access_key_id,
       aws_secret_access_key = secret_access_key,
       region_name='us-east-1'
    )
  return mturk

def writecsvfile(outputfile,rows,keys):
    '''Method to write the dictionary information in rows to a csv file passed as outputfile. keys is the list of keys for the dictionary'''
    with open(outputfile, 'w') as output:
        writer = csv.DictWriter(output,keys,restval="NA")
        writer.writeheader()
        writer.writerows(rows)

def writetxtfile(outputfile, rows):
    with open(outputfile,'w') as f:
        for item in rows:
            f.write("%s\n" % item)

def readfiles(files):
    '''Method to read in files passed in a list, concatenate all of their rows into one list rows and return that data'''
    rows = []
    for inputfile in files:
        with open(inputfile, 'r') as input:
            reader = csv.DictReader(input, next(input).split(","))
            rows.extend([row for row in reader])
    return rows

def writeIDS(inputfile,outputfile):
  rows = []
  with open(inputfile, 'r') as input:
    for i,line in enumerate(input):
      if i % 3 == 1:
        rows.append(line.split(" ")[2])

  writetxtfile(outputfile,rows)



# Arg 1 is the path to the file containing AWS credentials 
# Arg 2 should be the text file containing the conversations with turns separated by </s>, Arg 3 is desired output ID file name, 
# Arg 4 is desired output link file name, Arg 5 is desired outpout tracking file name
if __name__=="__main__":        
  main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
  #writeIDS(sys.argv[1],sys.argv[2])
  #getHIT(sys.argv[1],sys.argv[2])
  #DBDC('amt_all.csv')
  