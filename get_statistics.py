import psycopg2

def get_flow_data_from_db():
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        #print ("Database Connected....")
        cur = conn.cursor()
        cur.execute("select * from dim_flow where flow_creation_date BETWEEN '30-MAY-2018' AND '6-JUN-2018'")
        flow_data=cur.fetchall()
        conn.commit()
        return flow_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_stream_data_from_db(flow_id):
    try:
        conn=psycopg2.connect("dbname=bakk_dashboard user=bakk password=bakk_123 host=localhost")
        #print ("Database Connected....")
        cur = conn.cursor()
        cur.execute("select * from dim_stream where flow_id=%s and stream_end_time<'6-JUN-2018' order by stream_end_time desc"%(flow_id))
        stream_data=cur.fetchall()
        conn.commit()
        return stream_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

weekly_data=[]
fetched_flow_data=get_flow_data_from_db()
for line in fetched_flow_data:
    weekly_dict={}
    weekly_dict["week_no"]=15
    weekly_dict["flow_name"]=line[1]
    weekly_dict["start_date"]=line[2]
    flow_id=line[0]
    #print(flow_id)
    fetched_stream_data=get_stream_data_from_db(flow_id)
    status=[]
    for line in fetched_stream_data:
        status.append(line[4])
    if 'Running' in status:
        weekly_dict["status"]='Running'
        weekly_dict["end_date"]=''
    else:
        for line in fetched_stream_data:
            weekly_dict["end_date"]=line[3]
            weekly_dict["status"]=line[4]
    weekly_data.append(weekly_dict)
    print(weekly_dict)
        
        
