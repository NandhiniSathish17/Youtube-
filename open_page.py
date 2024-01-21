
from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd 
import datetime  
import streamlit as st
from PIL import Image





#API Connection String
def api_connection():
        Api_Id="AIzaSyC954eUepgdgh41ZcGhoNVrRVZZQJr_LB0"
        api_service="youtube"
        api_version="v3"

        youtube=build(api_service,api_version,developerKey=Api_Id)
        return youtube

youtube=api_connection()



#Get channel information
def get_channel_details(channel_id):
                request=youtube.channels().list(
                part='snippet,ContentDetails,statistics',
                id=channel_id
            )
                response=request.execute()

                for i in response['items']:
                    data=dict(channel_name=i['snippet']['title'],
                            channel_id=i['id'],
                            subscriber_data=i['statistics']['subscriberCount'],
                            views=i['statistics']['viewCount'],
                            total_videos=i['statistics']['videoCount'],
                            channel_description=i['snippet']['description'],
                            playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
                return data



#Get Video Details
def get_video_Id(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    nextpage_token=None
    while True:
        response_new=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=nextpage_token).execute()

        for i in range(len(response_new['items'])):

            video_ids.append(response_new['items'][i]['snippet']['resourceId']['videoId'])
            nextpage_token=response_new.get('nextPageToken')
        if nextpage_token is None:
            break
    return video_ids



#Get video Information
def get_video_Information(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
        )
        response = request.execute()

                
        for item in response['items']:
            data=dict(channel_title=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_id=item['id'],
                title=item['snippet']['title'],
                tags=item['snippet'].get('tags'),
                thumbnail=item['snippet']['thumbnails']['default']['url'],
                description=item['snippet'].get('description'),
                published_dt=item['snippet']['publishedAt'],
                duration=item['contentDetails']['duration'],
                view_count=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                comments=item['statistics'].get('commentCount'),
                fav_count=item['statistics']['favoriteCount'],
                definition=item['contentDetails']['definition'],
                caption=item['contentDetails']['caption']
                )
            video_data.append(data)

    return video_data 



#Get Comment Information
def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(
                comment_id=item['snippet']['topLevelComment']['id'],
                Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
            
                comment_data.append(data)
            
            
    except:
        pass
    return comment_data




#get_playlist_details
def get_Playlist_details(channel_id):
     All_data=[]
     next_page_token=None
     while True:
               request=youtube.playlists().list(
                    part='contentDetails,snippet',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token

               )
               response=request.execute()
               for item in response['items']:
                    data=dict(
                         Playlist_Id=item['id'],
                         Title=item['snippet']['title'],
                         Channel_Id=item['snippet']['channelId'],
                         Channel_Name=item['snippet']['channelTitle'],
                         PublishedAt=item['snippet']['publishedAt'],
                         Video_count=item['contentDetails']['itemCount'])
                    All_data.append(data)
                    
               next_page_token=response.get('nextPageToken')

               if next_page_token is None:
                    break     
     return All_data




#Connection to Mongodb
client=pymongo.MongoClient('mongodb://localhost:27017')
db=client['Youtube_data']



def channel_details(channel_id):
    ch_details=get_channel_details(channel_id)
    pl_details=get_Playlist_details(channel_id)
    vi_ids=get_video_Id(channel_id)
    vi_details=get_video_Information(vi_ids)
    com_details=get_comment_info(vi_ids)


    collection=db['channel_details']
    collection.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                           "video_information":vi_details,"comment_information":com_details})
    
    return "records uploaded sucessfully"




#Creating Tables for channels,playlist,videos,comments
def channel_table():

    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='admin123',database='Youtube')
    cur = myconnection.cursor()
    drop_query='''drop table if exists channels'''
    cur.execute(drop_query)
    myconnection.commit()
    # cur.execute("create database Youtube")

    try:
        create_query='''create table if not exists channels(
                                                    channel_name varchar(100),
                                                    channel_id varchar(100) primary key,
                                                    subscriber_data bigint,
                                                    views bigint,
                                                    total_videos int,
                                                    channel_description text,
                                                    playlist_Id varchar(80))'''
        cur.execute(create_query)
        myconnection.commit()
    except:
        print("channels table already created")

    ch_list=[]
    db=client['Youtube_data']
    collection=db['channel_details']

    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])

    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_name,
                                            channel_id,
                                            subscriber_data,
                                            views,
                                            total_videos,
                                            channel_description,
                                            playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['subscriber_data'],
                row['views'],
                row['total_videos'],
                row['channel_description'],
                row['playlist_Id'])
        try:
            cur.execute(insert_query,values)
            myconnection.commit()

        except:
            print("channels values already inserted")



    
def playlists_table():

        myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='admin123',database='Youtube')
        cur = myconnection.cursor()
        drop_query='''drop table if exists playlists'''
        cur.execute(drop_query)
        myconnection.commit()

        # cur.execute("create database Youtube")

     
        create_query='''create table if not exists playlists(
                                                        Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt varchar(50),
                                                        Video_count int)'''
                
        cur.execute(create_query)
        myconnection.commit()

        pl_list=[]
        db=client['Youtube_data']
        collection=db['channel_details']

        for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
                                for i in range(len(pl_data["playlist_information"])):
                                        pl_list.append(pl_data["playlist_information"][i])
        df1=pd.DataFrame(pl_list)


        for index,row in df1.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_count)

                        
                                                values(%s,%s,%s,%s,%s,%s)'''
                
                
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_count'])
                
        cur.execute(insert_query,values)
        myconnection.commit()



def video_table():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='admin123',database='Youtube')
    cur = myconnection.cursor()

    drop_query='''drop table if exists videos'''
    cur.execute(drop_query)
    myconnection.commit()
    
    create_query='''create table if not exists videos(channel_title varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(15) primary key,
                                                        title varchar(100),
                                                        tags text,
                                                        thumbnail varchar(200),
                                                        description text,
                                                        published_dt varchar(50),
                                                        duration varchar(50),
                                                        view_count bigint,
                                                        Likes bigint,
                                                        comments int,
                                                        fav_count int,
                                                        definition varchar(100),
                                                        caption varchar(100)
                                                        )'''
                


    cur.execute(create_query)
    myconnection.commit()

    vi_list=[]
    db=client['Youtube_data']
    collection=db['channel_details']

    for vi_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
         insert_query='''insert into videos(channel_title,
                                                        channel_id,
                                                        video_id,
                                                        title,
                                                        tags,
                                                        thumbnail,
                                                        description,
                                                        published_dt,
                                                        duration,
                                                        view_count,
                                                        Likes,
                                                        comments,
                                                        fav_count,
                                                        definition,
                                                        caption
                                                       )

                      
                           values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
         values=( row['channel_title'],
                  row['channel_id'],
                  row['video_id'],
                  row['title'],
                  row['tags'],
                  row['thumbnail'],
                  row['description'],
                  row['published_dt'],
                  row['duration'],
                  row['view_count'],
                  row['Likes'],
                  row['comments'],
                  row['fav_count'],
                  row['definition'],
                  row['caption']
                  )
        
    cur.execute(insert_query,values)
    myconnection.commit()



def comments_table():
        myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='admin123',database='Youtube')
        cur = myconnection.cursor()
        drop_query='''drop table if exists comments'''
        cur.execute(drop_query)
        myconnection.commit()

                # cur.execute("create database Youtube")

        
        create_query='''create table if not exists comments(comment_id varchar(100) primary key,
                                                                Video_Id varchar(50),
                                                                comment_Text text,
                                                                comment_Author varchar(50),
                                                                comment_Published varchar(50))'''
                        
        cur.execute(create_query)
        myconnection.commit()


        com_list=[]
        db=client['Youtube_data']
        collection=db['channel_details']

        for com_data in collection.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                   com_list.append(com_data["comment_information"][i])
        df3=pd.DataFrame(com_list)


        for index,row in df3.iterrows():
                                insert_query='''insert into comments(comment_id,
                                                                        Video_Id,
                                                                        comment_Text,
                                                                        comment_Author,
                                                                        comment_Published)

                                        
                                                                values(%s,%s,%s,%s,%s)'''
                                
                                
                                values=(row['comment_id'],
                                        row['Video_Id'],
                                        row['comment_Text'],
                                        row['comment_Author'],
                                        row['comment_Published']
                                        )
                                

                
        cur.execute(insert_query,values)
        myconnection.commit()


    

def tables():
    channel_table()
    playlists_table()
    video_table()
    comments_table()
    return "Tables Created Successfully"



def show_channel_table():
        ch_list=[]
        db=client['Youtube_data']
        collection=db['channel_details']

        for ch_data in collection.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data['channel_information'])

        df=st.dataframe(ch_list)

        return df


def show_playlists_table():
        pl_list=[]
        db=client['Youtube_data']
        collection=db['channel_details']

        for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1=st.dataframe(pl_list)

        return df1



def show_video_table():
    vi_list=[]
    db=client['Youtube_data']
    collection=db['channel_details']

    for vi_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2



def show_comment_table():
    com_list=[]
    db=client['Youtube_data']
    collection=db['channel_details']

    for com_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
                    com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    
    return df3
#Streamlit application


page_title="YouTube Data Harvesting and Warehousing"
page_icon="▶"
layout="centered"


st.set_page_config(page_title=page_title,page_icon=page_icon,layout=layout)
st.title(page_title +"  "+ page_icon)
st.image("youtube1.png",width=50)

st.balloons()
# img=Image.open("/Users/Shared/youtube/webpage/youtube.png")
# st.image(img,caption='youtude dataharvesting')

from streamlit_option_menu import option_menu

with st.sidebar:
    st.title(":red[Contents]")
   
    selected =option_menu( menu_title= "Overview",
          options=["Home","Collecting the data","Queries"],
          icons=["house-door","collection-play-fill","question-circle-fill"],
          menu_icon="book-fill",
          default_index=0)
         
          
    
    
    # st.header("Overview")
    # st.caption("Channel Scrapping")
    # st.caption('Collecting the Data')
    # st.caption('Mongodb')
    # st.caption('API Integration')
    # st.caption('Data Management using Mongodb and Sql')
if selected=="Home":
    st.text('''
Data harvesting is the process of gathering data from numerous sources, such as
websites, applications, and social media platforms, and storing it in a database in 
order to make assumptions.
Here we have created a Streamlit application that allows users to access and analyze data 
from multiple YouTube channels. The application should have the following features:
            
           1. Ability to input a YouTube channel ID and retrieve all the relevant data 
              (Channel name, subscribers, total video count, playlist ID, video ID, likes, 
              dislikes, comments of each video) using Google API.
           2. Option to store the data in a MongoDB database as a data lake.
           3. Ability to collect data for up to 10 different YouTube channels and store them 
              in the data lake by clicking a button.
           4. Option to select a channel name and migrate its data from the data lake to a 
              SQL database as tables.
           5. Ability to search and retrieve data from the SQL database using different
              search options, including joining tables to get channel details.''')


if selected=="Collecting the data":
      
    channel_id=st.text_input("Enter the Channel Id")

    if st.button("collect and store data"):
                ch_ids=[]
                db=client['Youtube_data']
                collection=db['channel_details']
                for ch_data in collection.find({},{"_id":0,"channel_information":1}):
                    ch_ids.append(ch_data['channel_information'])

                if channel_id in ch_ids:
                    st.success("details already exists")
                else:
                    insert=channel_details(channel_id)
                    st.success(insert)
    if st.button("Migrate to sql"):
            Table=tables()
            st.success(Table)

    show_table=st.radio("Choose table for Viewing",("CHANNELS",'VIDEOS',"PLAYLISTS","COMMENTS"))
    if show_table=="CHANNELS":
            show_channel_table()
    elif show_table=="PLAYLISTS":
            show_playlists_table()

    elif show_table=="VIDEOS":
            show_video_table()
    elif show_table=="COMMENTS":
                show_comment_table()

          

#Sql connection
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='admin123',database='Youtube')
cur = myconnection.cursor()
if selected=="Queries":

    questions=st.selectbox("Select your Query",("1.All videos in this channel",
                                                "2.Channels with most no of videos",
                                                "3.10 Most viewed videos",
                                                "4.Comments in each video",
                                                "5. Videos with highest likes",
                                                "6.Likes of all videos",
                                                "7.Views of each channels",
                                                "8.Videos published in the year of 2022",
                                                "9.Average duration of all videos in all channels",
                                                "10. Videos with highest number of comments"))


    if questions=="1.All videos in this channel":
        query1='''select title as videos,channel_title as channelname from videos'''
        cur.execute(query1)
        myconnection.commit()
        t1=cur.fetchall()
        df=pd.DataFrame(t1,columns=["Video title","Channel name"])
        st.write(df)

    elif questions=="2.Channels with most no of videos":
        query2='''select total_videos as no_videos,channel_name as channelname from channels  order by total_videos desc'''
        cur.execute(query2)
        myconnection.commit()
        t2=cur.fetchall()
        df2=pd.DataFrame(t2,columns=["Channel name","No.of videos",])
        st.write(df2)



    elif questions=="3.10 Most viewed videos":
        query3='''select view_count as views ,channel_title as channelname,title as videotitle from videos where view_count is not null  order by views desc limit 10'''
        cur.execute(query3)
        myconnection.commit()
        t3=cur.fetchall()
        df3=pd.DataFrame(t3,columns=["Views","Channel name","Video title",])
        st.write(df3)

    elif questions=="4.Comments in each video":
        query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
        cur.execute(query4)
        myconnection.commit()
        t4=cur.fetchall()
        df4=pd.DataFrame(t4,columns=["No.of comments","Videotitle"])

        st.write(df4)


    elif questions=="5. Videos with highest likes":
        query5='''select title as videotitle,channel_title as channelname,Likes as likescount from videos where Likes is not null order by likes desc'''
        cur.execute(query5)
        myconnection.commit()
        t5=cur.fetchall()
        df5=pd.DataFrame(t5,columns=["Videotitle","Channel name","Likescount"])
        st.write(df5)



    elif questions=="6.Likes of all videos":
        query6=''' select likes as likescount,title as videotitle from videos'''
        cur.execute(query6)
        myconnection.commit()
        t6=cur.fetchall()
        df6=pd.DataFrame(t6,columns=["Likescount","Videotitle"])
        st.write(df6)



    elif questions=="7.Views of each channels":
        query7='''select channel_name as channelname,views as totalviews from channels'''
        cur.execute(query7)
        myconnection.commit()
        t7=cur.fetchall()
        df7=pd.DataFrame(t7,columns=["Channelname","Totalviews"])
        st.write(df7)

    elif questions=="8.Videos published in the year of 2022":
        query8='''select title as video_title,published_dt as videoreleased,channel_title as channelname from videos '''
        cur.execute(query8)
        myconnection.commit()
        t8=cur.fetchall()
        df8=pd.DataFrame(t8,columns=["Videotitle","Publisheddate","Channelname"])
        st.write(df8)

    elif questions=="9.Average duration of all videos in all channels":
        query9='''select channel_title as channelname,avg(duration) as averageduration from videos group by channel_title'''
        cur.execute(query9)
        myconnection.commit()
        t9=cur.fetchall()
        df9=pd.DataFrame(t9,columns=["channelname","average_duration"])

    

        T9=[]
        for index,row in df9.iterrows():
                channel_title=row["channelname"]
                averageduration=row["average_duration"]
                average_duration_str=str(averageduration)
                T9.append(dict(channeltitle=channel_title,avgduration=averageduration))
        df1=pd.DataFrame(T9)
        st.write(df1)

    elif questions=="10. Videos with highest number of comments":
        query10='''select title as videotitle,channel_title as channelname,comments as comments from videos where comments is not null order by comments desc'''
        cur.execute(query10)
        myconnection.commit()
        t10=cur.fetchall()
        df10=pd.DataFrame(t10,columns=["Video tile","Channelname","Comments"])
        st.write(df10)









