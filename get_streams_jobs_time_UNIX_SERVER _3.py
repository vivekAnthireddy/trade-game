import cx_Oracle
from datetime import datetime
import paramiko
import psycopg2


def flow_id_stream_name(alias,fetched_data):
    for line in fetched_data:
        if(line["ALIAS"]==alias):
            stream_flow_id=line["FLOW_ID"]
            stream_name=line["STREAM_NAME"]
    return stream_flow_id,stream_name

def find_stream_names(log_file_data):
    stream_names=[]
    for line in log_file_data:
        if(line[7]=='JOB_STREAM'):
            flag=0
            if line[8] in stream_names:
                flag=1
            if(flag==0 and line[10]=='Successful'):
                line[12]=1
            if(line[10]=='Successful' or line[10]=='Blocked' or line[10]=='Running'):
                stream_names.append(line[8])
    return stream_names

def find_stream_times(stream,log_file_data):
    end_time=''
    start_time=''
    status='RERUN'
    for line in log_file_data:
        if(line[8]==stream and line[12]==1):
            status='SUCCESS'
        if(line[8]==stream and line[10]=='Successful' and end_time==''):
            end_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
        if(line[8]==stream and line[10]=='Running' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
        if(line[8]==stream and line[10]=='Running' and end_time=='' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
            status='RUNNING'
        if(line[8]==stream and line[10]=='Blocked' and end_time==''):
            end_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
            status='Blocked'
        elif(line[8]==stream and line[10]=='Blocked' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
    return start_time,end_time,status
    
def find_job_names(log_file_data):
    job_names=[]
    for line in log_file_data:
        if(line[7]=='JOB'):
            flag=0
            if line[8] in job_names:
                flag=1
            if(flag==0 and line[10]=='Successful'):
                line[12]=1
            if(line[10]=='Successful' or line[10]=='Error' or line[10]=='Running'):
                job_names.append(line[8])
    return job_names



    

def find_job_times(job,log_file_data):
    end_time=''
    start_time=''
    status='RERUN'
    for line in log_file_data:
        if(line[8]==job and line[12]==1):
            status='SUCCESS'
        if(line[8]==job and line[10]=='Successful' and end_time==''):
            end_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
        if(line[8]==job and line[10]=='Running' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
        if(line[8]==job and line[10]=='Running' and end_time=='' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
            status='RUNNING'
        if(line[8]==job and line[10]=='Error' and end_time==''):
            end_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
            line[8]='null'
            status='ERROR'
        elif(line[8]==job and line[10]=='Error' and start_time==''):
            start_time=str_to_date_time(line[1]+" "+line[2]+" "+line[5][:4]+" "+line[3])
    return start_time,end_time,status
        

def str_to_date_time(date_string):
    temp=date_string.split(' ')
    temp2=temp[3].split(':')
    if(int(temp2[0])>12):
        temp2[0]=int(temp2[0])%12
        temp2[0]=str(temp2[0])
        temp3=temp2[0]+':'+temp2[1]+':'+temp2[2]+'PM'
    elif(int(temp2[0])==12):
        temp3=temp2[0]+':'+temp2[1]+':'+temp2[2]+'PM'
    elif(int(temp2[0])==0):
        temp2[0]='12'
        temp3=temp2[0]+':'+temp2[1]+':'+temp2[2]+'AM'
    else:
        temp3=temp2[0]+':'+temp2[1]+':'+temp2[2]+'AM'
    temp4=temp[0]+' '+temp[1]+' '+temp[2]+' '+temp3
    date_string=temp4
    date_object=datetime.strptime(date_string,'%b %d %Y %I:%M:%S%p')
    return date_object


def load_jobs_to_database(data):
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        print ("Database Connected....")
        cur = conn.cursor()
        for row in data:
            if(len(row)==6):
                cur.execute("INSERT INTO public.dim_job VALUES('%s','%s','%s','%s','%s','%s')"%(row[0],row[1],row[2],row[3],row[4],row[5]))
            else:
                cur.execute("INSERT INTO public.dim_job(job_id,job_name,job_start_time,job_status,alias) VALUES('%s','%s','%s','%s','%s')"%(row[0],row[1],row[2],row[3],row[4]))
        conn.commit()
        print ("Jobs Data entered successfully...")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def load_streams_to_database(data):
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        print ("Database Connected....")
        cur = conn.cursor()
        for row in data:
            if(len(row)==7):
                cur.execute("INSERT INTO public.dim_stream VALUES('%s','%s','%s','%s','%s','%s','%s')"%(row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
            else:
                cur.execute("INSERT INTO public.dim_stream(stream_id,stream_name,stream_start_time,status,alias,flow_id) VALUES('%s','%s','%s','%s','%s','%s')"%(row[0],row[1],row[2],row[3],row[4],row[5]))
        conn.commit()
        print ("Streams Data entered successfully...")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def load_flows_to_database(data):
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        print ("Database Connected....")
        cur = conn.cursor()
        for row in data:
            cur.execute("INSERT INTO public.dim_flow VALUES('%s','%s','%s','%s')"%(row[0],row[1],row[2],row[3]))
        conn.commit()
        print ("Flows Data entered successfully...")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        
def get_required_details():
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        print ("Database Connected....")
        cur = conn.cursor()
        cur.execute("select last_run_date from fact_run_date")
        last_run_data=cur.fetchall()
        for row in last_run_data:
            last_run_date=str(row[0])

        return last_run_date
        conn.commit()
        print ("last run Data entered successfully...")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
if __name__== "__main__":

    last_run_date=get_required_details()
    date_now=datetime.now()
    present_run_date=date_now.strftime("%Y-%m-%d %H:%M:%S")
    print(last_run_date,present_run_date)
    print(type(present_run_date))
    
    try:
        conn=cx_Oracle.connect('toolamc/toolamc@indlin3909/TOOLS3I')
        cur=conn.cursor()

        flow_names=cur.execute("SELECT DISTINCT IMT_EVT.FLOW_NAME from IMT_EVT where IMT_EVT.RUN_USER='SCHED_ADMIN' and creation_date BETWEEN %s and %s"%(last_run_date,present_run_date))
        list_flow_names=[]
        for line in flow_names:
            list_flow_names.append(line[0])

        print(list_flow_names)
    
        labels=['FLOW_NAME','STREAM_NAME','ALIAS','EVENT_ID','CREATION_DATE']
        fetch_labels=cur.execute("SELECT IMT_EVT.FLOW_NAME,IMT_EVT.JOB_OR_STREAM,IMT_EVT.ALIAS,IMT_EVT.EVT_ID,IMT_EVT.CREATION_DATE from IMT_EVT WHERE IMT_EVT.RUN_USER='SCHED_ADMIN' and CREATION_DATE BETWEEN %s and %s"%(last_run_date,present_run_date))
        print("FETCHED DATA")
        fetched_data=[]
        for line in fetch_labels:
            my_dict={}
            for label,value in zip(labels,line):
                my_dict[label]=value
            fetched_data.append(my_dict)

        fetch_alias=cur.execute("SELECT DISTINCT IMT_EVT.ALIAS from IMT_EVT WHERE IMT_EVT.RUN_USER='SCHED_ADMIN' and creation_date BETWEEN %s and %s"%(last_run_date,present_run_date))
        alias_list=[line[0] for line in fetch_alias]
        cur.close()

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        if error.code == 955:
            print('Table already exists')
        if error.code == 1031:
            print("Insufficient privileges - are you sure you're using the owner account?")
        print(error.code)
        print(error.message)
        print(error.context)
        
    flow_id=0
    event_id=0
    for line in fetched_data:
        for flow_name in list_flow_names:
            if(line['FLOW_NAME']==flow_name and line['EVENT_ID']!=event_id):
                flow_id=flow_id+1
                event_id=line['EVENT_ID']
        line['FLOW_ID']=flow_id

    flows_data=[]
    temp_flow_id=[]
    for line in fetched_data:
        flow_data=[]
        if line['FLOW_ID'] not in temp_flow_id:
            temp_flow_id.append(line['FLOW_ID'])
            flow_data.append(line['FLOW_ID'])
            flow_data.append(line['FLOW_NAME'])
            flow_data.append(line['CREATION_DATE'])
            flow_data.append(1)
            flows_data.append(flow_data)


    try:
        hostname='indlin3905'
        username='imt3905'
        password='Unix_11'


        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, username=username, password=password, look_for_keys=False, allow_agent=False)
        print("connection established")

        jobs_data=[]
        job_id=0
        stream_id=0
        streams_data=[]
        for current_alias in alias_list:
            command="cd TWA/TWS/stdlist/logs && find . -type f -name 'imt_event_handler_*.log' | xargs grep -l %s |sort -r"%(current_alias)
            stdin,stdout,stderr=client.exec_command(command)
            log_file_data=[]
            for line in stdout.readlines():
                log_file=line
                str1=log_file[2:]

                command="cd TWA/TWS/stdlist/logs && cat %s"%(str1)
                stdin,stdout,stderr=client.exec_command(command)
                output_data=stdout.readlines()
                output_data.sort(reverse=True)
                

                for line in output_data:
                    line=line.split(' ')
                    for alias in line:
                        if (alias==current_alias):
                            if(len(line)==13):
                                del(line[2])
                            line.append(0)
                            log_file_data.append(line)

            job_names=find_job_names(log_file_data)
            for job in job_names:
                job_data=[]
                start_time,end_time,status=find_job_times(job,log_file_data)
                if(start_time !=''):
                    job_id=job_id+1
                    job_data.append(job_id)
                    job_data.append(job)
                    job_data.append(start_time)
                    if(end_time!=''):
                        job_data.append(end_time)
                    job_data.append(status)
                    job_data.append(current_alias)
                    jobs_data.append(job_data)

            stream_names=find_stream_names(log_file_data)
            for stream in stream_names:
                stream_data=[]
                start_time,end_time,status=find_stream_times(stream,log_file_data)
                if(start_time!=''):
                    stream_id=stream_id+1
                    stream_data.append(stream_id)
                    stream_flow_id,stream_name=flow_id_stream_name(current_alias,fetched_data)
                    stream_data.append(stream_name)
                    stream_data.append(start_time)
                    if(end_time!=''):
                        stream_data.append(end_time)
                    stream_data.append(status)
                    stream_data.append(current_alias)
                    stream_data.append(stream_flow_id)
                    streams_data.append(stream_data)
    except Exception as e:
        print(e.args)
    client.close()


    load_flows_to_database(flows_data)
    load_streams_to_database(streams_data)
    load_jobs_to_database(jobs_data)







