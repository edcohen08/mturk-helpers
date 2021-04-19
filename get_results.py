import boto3
import xmltodict
import sys
import csv
import json

def main(credsfile, sandbox, HITIDsfile,results_file):

    ''' Code to get results from a HIT published on Mechanical Turk. 
    Code is courtesy of AWS at 
    https://blog.mturk.com/tutorial-a-beginners-guide-to-crowdsourcing-ml-training-data-with-python-and-mturk-d8df4bdf2977
    Args HITIDsfile: this should be given when the code is run and is the path to a file containing the HITIDS, separated line by line, for all the HITs 
    you wish to retrieve results for.'''


    
	# You will need the following library
	# to help parse the XML answers supplied from MTurk
	# Install it in your local environment with
	# pip install xmltodict


    mturk = connectToMTurk(credsfile,sandbox)


    keys = ['HITId', 'WorkerId', 'Answer', 'SubmitTime','AutoApprovalTime','ApprovalTime', 'AcceptTime', 'AssignmentStatus', 'AssignmentId']

    with open(HITIDsfile,'r') as input:

        ''' This loop is to get the results for every HIT that was published, so it checks the HITID for every HITID in the file.'''
        all_results = []    # this empty list will be filled with the Assignment result dictionaries provided by AWS. We will use this to write our output csv.
        for line in input:
            hit_id = line.strip()

            worker_results = mturk.list_assignments_for_hit(HITId=hit_id, AssignmentStatuses=['Submitted','Approved'], MaxResults=30) # We only want HITs that have been 'Submitted' and thus have been completed by workers. 

            if worker_results['NumResults'] > 0:    # we only want to look at HITs that have a nonzero amount of results, otherwise the HIT remains to be done
               for assignment in worker_results['Assignments']:   # HITs can be published to be completed by multiple workers and we want to check the results for each worker who completes the HIT
                  xml_doc = xmltodict.parse(assignment['Answer'])
                  print("Worker's answer was:")
                  if type(xml_doc['QuestionFormAnswers']['Answer']) is list:
                     # Multiple fields in HIT layout
                     for answer_field in xml_doc['QuestionFormAnswers']['Answer']:      # loops through every input field that the worker gavea response for
                        print("For input field: " + answer_field['QuestionIdentifier'])
                        #print("Submitted answer: " + answer_field['FreeText'])
                        assignment[answer_field['QuestionIdentifier']] = answer_field['FreeText']  # adds to the assignment dicionary the workers response to each input field
                        identifier = answer_field['QuestionIdentifier']
                        if identifier not in keys:
                          keys.append(identifier)
                  else:
                     # One field found in HIT layout
                     print("For input field: " + xml_doc['QuestionFormAnswers']['Answer']['QuestionIdentifier'])
                     print("Submitted answer: " + xml_doc['QuestionFormAnswers']['Answer']['FreeText'])
                     assignment[xml_doc['QuestionFormAnswers']['Answer']['QuestionIdentifier']] = xml_doc['QuestionFormAnswers']['Answer']['FreeText']
                  #taskAnswers = assignment['taskAnswers']
                  #taskAnswersdict = json.loads(taskAnswers[1:len(taskAnswers)-1])
                  # for num in taskAnswersdict:
                  #   for label in taskAnswersdict[num]:
                  #     if num+'.'+label not in keys:
                  #       keys.append(num+'.'+label)
                  #     assignment[num+'.'+label] = taskAnswersdict[num][label]
                  all_results.append(assignment)
            else:
               print("No results ready yet")

        sorted_results = sorted(all_results, key=lambda k: k['HITId'], reverse=True)
        print (len(sorted_results))

        writecsvfile(results_file,sorted_results,keys)


def connectToMTurk(credsfile, sandbox):
  with open(credsfile, 'r') as creds_input:
      for i,line in enumerate(creds_input):
        if i==1:
          comma_split = line.split(",")
          access_key_id = comma_split[0].strip()
          secret_access_key = comma_split[1].strip()

  if sandbox=='True' or sandbox=='true':
    MTURK_SANDBOX = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    mturk = boto3.client('mturk',
       aws_access_key_id = access_key_id,
       aws_secret_access_key = secret_access_key,
       region_name='us-east-1',
       endpoint_url = MTURK_SANDBOX
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



if __name__=="__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
