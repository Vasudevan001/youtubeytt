from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#Api_key Connection

def Api_Connect():
    Api_Key="AIzaSyBkeO0ia6LhWVtZJ35bt1dLoU25_FonZzo"
    ApiService_name="Youtube"
    Api_Version="v3"

    youtube=build(ApiService_name,Api_Version,developerKey=Api_Key)

    return youtube

youtube=Api_Connect()


#Get Channel Information

def get_channel_details(channel_id):
    request=youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id

    )
    response=request.execute()

    for i in response["items"]:
        data=dict(channel_Name = i ["snippet"]["title"],
                 channel_Id    = i ["id"],
                 Subscribers   = i ["statistics"]["subscriberCount"],
                 views         = i["statistics"]["viewCount"],
                 Total_Videos  = i["statistics"]["videoCount"],
                 Channel_Description=i["snippet"]["description"],
                 Playlist_Id   = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
    


#Get Video_ids

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(
                                        id=channel_id,
                                        part="contentDetails").execute()
    Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None
    while True:

        response1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_info=[]
    for i in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
        response=request.execute()

        for i in response["items"]:
            data=dict(Channel_Name=i["snippet"]["channelTitle"],
                      Channl_Id=i["snippet"]["channelId"],
                      video_Id=i["id"],
                      Video_title=i["snippet"]["title"],
                      Tags=i["snippet"].get("tags"),
                      Thumnail=i["snippet"]["thumbnails"]["default"]["url"],
                      Description=i["snippet"].get("description"),
                      Pulished_Date=i["snippet"]["publishedAt"],
                      Duration=i["contentDetails"]["duration"],
                      Views=i["statistics"].get("viewCount"),
                      Likes=i["statistics"].get("likeCount"),
                      Comments=i["statistics"].get("commentCount"),
                      Favorite_Count=i["statistics"]["favoriteCount"],
                      Definition=i["contentDetails"]["definition"],
                      Caption_Status=i["contentDetails"]["caption"])
            video_info.append(data)
    return video_info


#get comment information
def comment_info(video_ids):
    comment_details=[]
    try:
        for i in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=i,
                maxResults=50
            )
            response=request.execute()

            for i in response['items']:
                data=dict(Comment_Id=i["snippet"]["topLevelComment"]["id"],
                        Video_Id=i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                                )
                comment_details.append(data)
    except:
        pass
    return comment_details

#get Playlist details
def Get_Playlist_id(channel_id):
    next_page_token=None
    all_playlist=[]

    while True:

        request=youtube.playlists().list(
                                            part="snippet,contentDetails",
                                            channelId=channel_id,
                                            maxResults=50,
                                            pageToken=next_page_token
        )

        response=request.execute()

        for i in response["items"]:
            data=dict(Playlist_Id=i["id"],
                    Title=i["snippet"]["title"],
                    Channel_Id=i["snippet"]["channelId"],
                    Channel_Name=i["snippet"]["channelTitle"],
                    publishedAt=i["snippet"]["publishedAt"],
                    Video_Count=i["contentDetails"]["itemCount"])
            all_playlist.append(data)
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return all_playlist


#Connect Mongodb

client=pymongo.MongoClient("mongodb+srv://vasudevdoc:1234@cluster0.zfa3dak.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]
coll1=db["Channel_Details"]


def Channel_details(channel_id):
    ch_details= get_channel_details(channel_id)
    pl_list=Get_Playlist_id(channel_id)
    vi_id=get_video_ids(channel_id)
    video_info=get_video_info(vi_id)
    cmt_info=comment_info(vi_id)

    coll1=db['channel_details']
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_list,
                      "video_information":video_info,
                      "comment_information":cmt_info})
    return "upload completed"

#Table creation for channels,playlists,videos,comments
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vasu",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            channel_Id varchar(100) primary key,
                                                            Subscribers bigint,
                                                            views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(100)
                                                            )'''
        
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channel Table already Created")

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(i["channel_information"])

    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_Name,
                                            channel_Id,
                                            Subscribers,
                                            views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["channel_Name"],
                row["channel_Id"],
                row["Subscribers"],
                row["views"],
                row['Total_Videos'],
                row["Channel_Description"],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Channels Values are already")
    
    


def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vasu",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()
    drop_query='''drop table if exists Playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists Playlists(Playlist_Id varchar(100),
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        publishedAt timestamp,
                                                        Video_Count int
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    Pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            Pl_list.append(i["playlist_information"][j])

    df1=pd.DataFrame(Pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into Playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            publishedAt,
                                            Video_Count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row["Playlist_Id"],
                row["Title"],
                row["Channel_Id"],
                row["Channel_Name"],
                row['publishedAt'],
                row["Video_Count"])
        
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("playlist are already inserted")
    

def videos_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vasu",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channl_Id varchar(100),
                                                    video_Id varchar(40) primary key,
                                                    Video_title varchar(200),
                                                    Tags text,
                                                    Thumnail varchar(200),
                                                    Description text,
                                                    Pulished_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"video_information":1}):
        for j in range(len(i["video_information"])):
            vi_list.append(i["video_information"][j])

    df3=pd.DataFrame(vi_list)

    for index,row in df3.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channl_Id,
                                            video_Id,
                                            Video_title,
                                            Tags,
                                            Thumnail,
                                            Description,
                                            Pulished_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["Channel_Name"],
                row["Channl_Id"],
                row["video_Id"],
                row["Video_title"],
                row['Tags'],
                row["Thumnail"],
                row["Description"],
                row["Pulished_Date"],
                row["Duration"],
                row["Views"],
                row['Likes'],
                row["Comments"],
                row["Favorite_Count"],
                row['Definition'],
                row["Caption_Status"])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("video infomartion are already")
    
        
        


def comment_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vasu",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comment'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comment(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(100),
                                                        Comment_Published timestamp
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    comment_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            comment_list.append(i["comment_information"][j])

    df4=pd.DataFrame(comment_list)

    for index,row in df4.iterrows():
        insert_query='''insert into comment(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published
                                            )
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values=(row["Comment_Id"],
                row["Video_Id"],
                row["Comment_Text"],
                row["Comment_Author"],
                row['Comment_Published'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("comment Values are already")
           
def Tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_table()

    return "Tables are Created"

    
def show_chennals_tables():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(i["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    Pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            Pl_list.append(i["playlist_information"][j])

    df1=st.dataframe(Pl_list)

    return df1

def Show_videos_tables():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"video_information":1}):
        for j in range(len(i["video_information"])):
            vi_list.append(i["video_information"][j])

    df3=st.dataframe(vi_list)

    return df3

def show_comment_table():
    comment_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            comment_list.append(i["comment_information"][j])

    df4=st.dataframe(comment_list)

    return df4


#Sreamlit Part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUING]")
    st.header("Skill Take Away")
    st.caption("python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

Channel_Id=st.text_input("Enter Channel ID:")

if st.button("collection and store data"):
    channels_id=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for i in coll1.find({},{"_id":0,"channel_information":1}):
        channels_id.append(i["channel_information"]["channel_Id"])
    
    if Channel_Id in channels_id:
        st.success("Channels Details of the given Channel id already exists")

    else:
        insert=Channel_details(Channel_Id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=Tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_chennals_tables()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    Show_videos_tables()

elif show_table=="COMMENTS":
    show_comment_table()
    




    


    

#SQL Connection
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vasu",
                        database="youtube_data",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select Your Question",("1.What are the names of all the videos and their corresponding channels?",
                                        "2.Which channels have the most number of videos, and how many videos do they have?",
                                        "3.What are the top 10 most viewed videos and their respective channels?",
                                        "4.How many comments were made on each video, and what are their corresponding video names?",
                                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                        "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                        "8.What are the names of all the channels that have published videos in the year 2022?",
                                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

   

if question=="1.What are the names of all the videos and their corresponding channels?":
    q1=('''select Video_title as videos,Channel_Name as Channelname from videos''')
    cursor.execute(q1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)


elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    q2=('''select channel_name as channelname,Total_Videos as no_videos from channels order by total_videos desc''')
    cursor.execute(q2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2, columns=["channel name", "no of videos"])
    st.write(df1)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    q3 = ('''SELECT views as views, channel_Name as channelname, Video_title as videotitle FROM videos
                ORDER BY views DESC LIMIT 10''')

    cursor.execute(q3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])

    st.write("Top 10 most viewed videos and their respective channels:")
    st.dataframe(df3)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    q4 = ('''SELECT comments as no_comments, Video_title as videotitle from videos where comments is not null''')


    cursor.execute(q4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])

    st.write("Number of comments for each video:")
    st.dataframe(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        q5 = '''SELECT Video_title AS videotitle, channel_Name AS channelname, Likes AS likecount
                FROM videos WHERE Likes IS NOT NULL ORDER BY Likes DESC '''



        cursor.execute(q5)
        mydb.commit()
        t9 = cursor.fetchall()
        df5 = pd.DataFrame(t9, columns=["videotitle", "channelname", "likecount"])
        st.write("Videos with the highest number of likes:")
        st.dataframe(df5)
        mydb.rollback()  # Rollback any previous transactions

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    q6='''select Likes as likecount , Video_title as videotitle from videos'''
    cursor.execute(q6)
    mydb.commit()
    t10=cursor.fetchall()
    df6=pd.DataFrame(t10,columns=["likescount","videotitle"])
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    q7='''select  channel_Name as channelname , views as totalviews from channels'''
    cursor.execute(q7)
    mydb.commit()
    t11=cursor.fetchall()
    df7=pd.DataFrame(t11,columns=["channelname","totalviews"])
    st.write(df7)
    mydb.rollback()

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    q8='''select  Video_title as videotitle ,videos.Pulished_Date as videorelease,channel_Name as channelname from videos
            where extract(year from videos.pulished_date)=2022'''
    cursor.execute(q8)
    mydb.commit()
    t12=cursor.fetchall()
    df8=pd.DataFrame(t12,columns=["videotitle","videorelease","channelname"])
    st.write(df8)
    mydb.rollback()

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    q9='''select channel_Name as channelname , AVG (duration)as avgduration from videos group by channel_Name '''
    cursor.execute(q9)
    mydb.commit()
    t13=cursor.fetchall()
    df9=pd.DataFrame(t13,columns=["channelname","avgduration"])
    st.write(df9)
    mydb.rollback()

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    q10='''select Video_title as videotitle , channel_Name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(q10)
    mydb.commit()
    t14=cursor.fetchall()
    df10=pd.DataFrame(t14,columns=["videotitle","channelname","comments"])
    st.write(df10)
    mydb.rollback()