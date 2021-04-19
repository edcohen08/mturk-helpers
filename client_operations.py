import boto3
import sys
import csv
import itertools
from datetime import datetime

def main(credsfile,input,output):
    mturk = connectToMTurk(credsfile, False)
    print (mturk.get_account_balance())
    
def listHITS(client):
    result = client.list_reviewable_hits(MaxResults=100)
    HITs = result['HITs']
    next_token = result['NextToken']
    print (next_token)
    Ids = []
    for HIT in HITs:
        Ids.append(HIT['HITId'])
    writetxtfile('HITIDS_out.txt', Ids)
    #writecsvfile('all_HITs.csv',HITs,list(HITs[0].keys))

def deleteHITs(client, HITs):
    for HIT in HITs:
        client.delete_hit(HITId=HIT)

def connectToMTurk(credsfile, sandbox):

    #with open(credsfile, 'r') as creds_input:
    #    for i,line in enumerate(creds_input):
    #        if i==1:
    #            comma_split = line.split(",")
    #            access_key_id = comma_split[0].strip()
    #            secret_access_key = comma_split[1].strip()


    if sandbox:
        MTURK_SANDBOX = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
        mturk = boto3.client('mturk',
            aws_access_key_id = 'YOUR ACCESS KEY HERE',
            aws_secret_access_key = 'YOUR SECRET KEY HERE',
            region_name='us-east-1',
            endpoint_url = MTURK_SANDBOX  # to access MTurk marketplace leave out endpoint_url completely
        )
    else:
        mturk = boto3.client('mturk',
            aws_access_key_id = 'YOUR ACCESS KEY HERE',
            aws_secret_access_key = 'YOUR SECRET KEY HERE',
            region_name='us-east-1'
        )
    
    return mturk


def send_bonus(client, rows, output):

    workers = []
    for row in rows:
        if row['WorkerId'] not in workers:
            workers.append(row['WorkerId'])
    worker_dicts = []
    for worker in workers:
        worker_dict = dict.fromkeys(['WorkerId','HITs', 'AssignmentId'])
        worker_dict['WorkerId'] = worker
        worker_dict['HITs'] = 0
        worker_dict['AssignmentId'] = ""
        for row in rows:
            worker_ID = row['WorkerId']
            if worker_ID==worker:
                worker_dict['HITs']+=1
                worker_dict['AssignmentId'] = row['AssignmentId']
        worker_dicts.append(worker_dict)
    bonuses_given = []
    for worker in worker_dicts:
        if worker['HITs']==60 or worker['WorkerId']=='A2VFHTZKUFKG16':
            print('Giving worker {} a bonus of 12 for their work on assignment {}'.format(worker['WorkerId'], worker['AssignmentId']))
            bonus = "12.00"
            reason = "This bonus is being paid out because we realized we were not paying fairly for the time and work required and we hope this is proper compensation for the work you gave us. Thank you for your time and work in completing these HITs. We hope you enjoyed them and will assist us again in the future. "
            client.send_bonus(WorkerId=worker['WorkerId'],AssignmentId=worker['AssignmentId'], BonusAmount=bonus,Reason=reason)
            bonuses_given.append(worker['WorkerId'])


    writetxtfile(output,bonuses_given)

def approveAssignments(client,HITs):

    for assignment in HITs:
        client.approve_assignment(
            AssignmentId = assignment['AssignmentId'],
            RequesterFeedback = "Great job, thank you for your time and work."
        )
        print("Approved assignment {}".format(assignment['AssignmentId']))

def updateExpiration(client, HITs):
    for HIT in HITs:
        client.update_expiration_for_hit(HITId=HIT.strip(), ExpireAt=datetime(2019, 8, 21))


def createAdditionalAssignments(client,HITs):
    for HIT in HITs:
        num_HITs = int(HIT['NumberOfAssignmentsAvailable']) + int(HIT['NumberOfAssignmentsCompleted\n'])
        num_more = 15 - num_HITs
        client.create_additional_assignments_for_hit(HITId=HIT['HITId'],NumberOfAdditionalAssignments=num_more)
        print ("created {} more assignments for HIT {}".format(num_more,HIT['HITId']))

def messageWorker(client, workers, subject, message):
    print (len(workers))
    # requires that you have approved or rejected work from this worker before
    client.notify_workers(Subject=subject,MessageText=message,WorkerIds=workers)
    print("Message sent to workers")

def createAndAssignQualification(client,name,keywords,description,status,workers):

    custom_qualification = client.create_qualification_type(Name=name,Keywords=keywords,Description=description,QualificationTypeStatus=status)
    qualificationtypeid = custom_qualification['QualificationType']['QualificationTypeId']

    assignQualification(client,qualificationtypeid,workers)

    return qualificationtypeid

def assignQualification(client, qualificationtypeid, workers):
    for worker in workers:
        client.associate_qualification_with_worker(QualificationTypeId=qualificationtypeid,WorkerId=worker.strip(),IntegerValue=1)    
        print ("gave worker {} qualification {}".format(worker,qualificationtypeid))

def updateQualification(client, qualificationtypeid, description):
    client.update_qualification_type(QualificationTypeId=qualificationtypeid,Description=description)


def getHITs(client,HITIDs,output):

    HITsinfo = []
    for HITID in HITIDs:
        HIT = client.get_hit(HITId=HITID.strip())
        HITsinfo.append(HIT['HIT'])

    sorteddd = sorted(HITsinfo, key = lambda i: i['CreationTime'])
    writecsvfile(output,sorteddd,list(HITsinfo[0].keys()))

def getQualificationType(client,qualificationtypeid):
    print(client.get_qualification_type(QualificationTypeId=qualificationtypeid))

def getWorkersWithQual(client,qualificationtypeid,output):
    result = client.list_workers_with_qualification_type(QualificationTypeId=qualificationtypeid,MaxResults=100)
    for item in result['Qualifications']:
        print(item['WorkerId'])

def readcsvfile(file):
    rows = []
    with open(file, 'r') as input:
        reader = csv.DictReader(input, next(input).split(","))
        rows.extend([row for row in reader])  
    return rows

def readtxtfile(file):
    lines = []
    with open(file, 'r') as input:
        lines.extend([line.strip() for line in input])
    return lines

def writecsvfile(output,rows,keys):

    with open(output, 'w') as output:
        writer = csv.DictWriter(output,keys,restval="NA")
        writer.writeheader()
        writer.writerows(rows)

def writetxtfile(output,rows):
    with open(output,'w') as f:
        for item in rows:
            f.write("%s\n" % item)

if __name__=="__main__":
    main(sys.argv[1],sys.argv[2],sys.argv[3])
