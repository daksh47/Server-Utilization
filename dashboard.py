import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import os, re, time
import sys
from pathlib import Path
sys.path.extend([str(Path(__file__).resolve().parent.parent)])
from connectors import data_fetcher # Your original data fetcher
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

st.set_page_config(
    page_title="Server Utilization",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Server Utilization")
st.markdown("Last Updated 10th Sept 2025 03:15pm")

def display_controls():
    """Renders all the input widgets and returns their current values."""
    c1, c2 = st.columns(2)
    start_day = None
    with c2:
        end_day = st.date_input(
            "End Day",
            value=start_day if start_day else date.today(),
            key="end_day"
            ,max_value=date.today()
        )
    with c1:
        start_day = st.date_input("Start Day", value=date.today() - timedelta(days=6), key="start_day"
        , max_value=date.today()
        )
    
    if start_day > end_day:
        st.error("Note : Start date must be before end date.")
        return None

    left_col, right_col = st.columns([1, 1])
    selected_tables = []
    with left_col:
        # NOTE: The value for "Table1" is the actual table name 'scraper_run'
        # table_choice = st.checkbox("Table", ["scraper_run", "processing_log_test"], horizontal=True, key="table_choice")
        st.write("**Select Tables:**")
        cb1, cb2 = st.columns(2)
        with cb1:
            scraper_selected = st.checkbox("scraper_run", value=True, key="scraper_choice")
        with cb2:
            processing_selected = st.checkbox("processing_log_test", value=False, key="processing_choice")

        if scraper_selected:
            selected_tables.append("scraper_run")
        if processing_selected:
            selected_tables.append("processing_log_test")

        if not selected_tables:
            st.error("Note : Please select at least one table.")
            return None

        # --- CHANGE 2: Minimum of one checkbox should be checked ---
        if not selected_tables:
            st.error("Note : Please select at least one table.")
            return None
    with right_col:
        # NOTE: We map user-friendly names to integer values for the query
        server_choice = st.radio("Server", ["Server 1", "Server 2", "Server 3", "Server 4"],horizontal=True, key="server_choice")

    return start_day, end_day, selected_tables, server_choice

def ordinal_suffix(day):
    return 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')

def time_to_seconds(time_str):
    """Converts a 'HH:MM:SS' string to total seconds."""
    if not isinstance(time_str, str):
        return 0 # Or handle error appropriately
    try:
        h, m, s = map(int, time_str.split(':'))
        delta = timedelta(hours=h, minutes=m, seconds=s)
        return int(delta.total_seconds())
    except ValueError:
        # Handles cases where the string might be malformed
        return 0

def get_data(start_date, end_date, tables, server):
    """Fetches and processes data based on user selections."""
    
    # Map the user-friendly server name to the integer value for the query
    server_map = {"Server 1": 1, "Server 2": 2, "Server 3": 3, "Server 4":4}
    server_id = server_map.get(server, 1) # Default to 1 if not found

    # Construct the query dynamically
    final_data = {
        "scraper_run":[],
        "processing_log_test":[]
    }
    for table in tables:
        if table == "scraper_run":
            query = f"""
            SELECT * 
            FROM igamingcompass.scraper_run as sc
            JOIN igamingcompass.operator_sites as os
            ON sc.operator_site_id = os.id 
            WHERE date(sc.created_at + INTERVAL 330 MINUTE ) >= '{start_date.strftime('%Y-%m-%d')}'
            AND date(sc.created_at + INTERVAL 330 MINUTE ) <= '{end_date.strftime('%Y-%m-%d')}'
            AND os.server_conf = {server_id}
            ORDER BY sc.created_at DESC;
            """
        else:
            query = f"""
            with cte as (
            select * from igamingcompass.processing_log_test
            where server = {server_id} and date(created_at + INTERVAL 330 MINUTE ) >= '{start_date.strftime('%Y-%m-%d')}'
            AND date(created_at + INTERVAL 330 MINUTE ) <= '{end_date.strftime('%Y-%m-%d')}'
            and message = 'processing_start' order by created_at
            ),
            cte1 as (
            select * from igamingcompass.processing_log_test
            where server = {server_id} and date(created_at + INTERVAL 330 MINUTE ) >= '{start_date.strftime('%Y-%m-%d')}'
            AND date(created_at + INTERVAL 330 MINUTE ) <= '{end_date.strftime('%Y-%m-%d')}'
            and message = 'processing_end' order by created_at
            )
            select c1.operator_site_id, (c1.created_at) as start_time, 
            ifnull(c2.created_at,"FAILED_SCRIPT") as end_time 
            from cte c1
            left join cte1 c2 on c1.run_id = c2.run_id
            order by start_time desc;
            """
        site_details_df = data_fetcher(query, [])

        if site_details_df.empty:
            st.warning("No data found for the selected criteria.")
            return pd.DataFrame()

        data1 = []

        tm_start_date = date(int(str(start_date).split("-")[0].strip()), int(str(start_date).split("-")[1].strip()), int(str(start_date).split("-")[2].strip()))
        tm_end_date = date(int(str(end_date).split("-")[0].strip()), int(str(end_date).split("-")[1].strip()), int(str(end_date).split("-")[2].strip()))

        all_dates = dict()
        all_dates_count = dict()
        while tm_start_date <= tm_end_date:
            all_dates[str(tm_start_date)]=set()
            all_dates_count[str(tm_start_date)]=0
            tm_start_date += timedelta(days=1)

        if table != "scraper_run":
            site_details_df['end_time_1'] = site_details_df['end_time'].copy()
            site_details_df.loc[site_details_df['end_time'] == 'FAILED_SCRIPT', 'end_time'] = pd.to_datetime(site_details_df['start_time']) + pd.Timedelta(minutes=10)
            site_details_df['end_time'] = pd.to_datetime(site_details_df['end_time']) + pd.Timedelta(hours=5, minutes=30)
            site_details_df['start_time'] = pd.to_datetime(site_details_df['start_time']) + pd.Timedelta(hours=5, minutes=30)


        for row in site_details_df.itertuples():
            start_dt = row.start_time
            end_dt = row.end_time
            site_id = row.operator_site_id
            status =  ('1' if row.data_gathering and row.data_verification and row.record_count else '0') if table == "scraper_run" else ('1' if row.end_time_1 != "FAILED_SCRIPT" else '-1')

            start_date_str = f"{start_dt.day}{ordinal_suffix(start_dt.day)} {start_dt.strftime('%b %Y')}"
            
            if start_dt.date() != end_dt.date():
                finish_time_for_start_day = "23:59:59"
                if end_dt.date() <= end_date:
                    end_date_str = f"{end_dt.day}{ordinal_suffix(end_dt.day)} {end_dt.strftime('%b %Y')}"
                    data1.append({
                        "Day": end_date_str,
                        "Start": "00:00:00",
                        "Finish": end_dt.strftime('%H:%M:%S'),
                        "Operator_site_id": site_id,
                        "Status":status
                    })
            else:
                finish_time_for_start_day = end_dt.strftime('%H:%M:%S')


            data1.append({
                "Day": start_date_str,
                "Start": start_dt.strftime('%H:%M:%S'),
                "Finish": finish_time_for_start_day,
                "Operator_site_id": site_id,
                "Status":status
            })


            start = str(row.start_time)
            end = str(row.end_time)

            start_time = time_to_seconds(start.split(" ")[1].strip())
            end_time = time_to_seconds(end.split(" ")[1].strip())

            if start.split(" ")[0].strip() == end.split(" ")[0].strip():
                if start.split(" ")[0].strip() in all_dates:
                    se = all_dates[start.split(" ")[0].strip()]
                    if end_time >= 86400:
                        end_time = 86399
                    for i in range(start_time,end_time+1):
                        se.add(i)
                    all_dates[start.split(" ")[0].strip()] = se
                    all_dates_count[start.split(" ")[0].strip()] += 1
            else:
                if start.split(" ")[0].strip() in all_dates:
                    se = all_dates[start.split(" ")[0].strip()]
                    for i in range(start_time,86400):
                        se.add(i)
                    all_dates[start.split(" ")[0].strip()] = se
                    all_dates_count[start.split(" ")[0].strip()] += 1

                if end.split(" ")[0].strip() in all_dates:
                    if end_dt.date() <= end_date:
                        se = all_dates[end.split(" ")[0].strip()]
                        if end_time >= 86400:
                            end_time = 86399
                        for i in range(0,end_time+1):
                            se.add(i)
                        all_dates[end.split(" ")[0].strip()] = se
                        all_dates_count[end.split(" ")[0].strip()] += 1
        final_data[table] = [pd.DataFrame(data1), all_dates, all_dates_count]

        # usuage = dict()
        # count = dict()
        # for i in data1:
        #     start = time_to_seconds(i["Start"])
        #     end = time_to_seconds(i["Finish"])
        #     li = usuage.get(i['Day'],set())
        #     count[i['Day']] = count.get(i['Day'],0)+1
        #     if end >=86400:
        #         end = 85999
        #     for j in range(start, end+1):
        #         li.add(j)
        #     usuage[i['Day']] = li
        
        # final_data[table]=[pd.DataFrame(data1), usuage, count]
    
    return final_data

def display_chart(df1,start_date,end_date):
    """Renders the Plotly timeline chart."""
    both= False
    df= pd.DataFrame()

    all_dates_in_range = pd.date_range(start=start_date, end=end_date, freq='D')
    table_names = [] # In descending order of preference

    data_present = False

    if 'scraper_run' in df1 and df1['scraper_run'] and len(df1['scraper_run']) !=0:
        data_present=True
        df = df1['scraper_run'][0]
        df['TaskID'] = df.index.astype(str)
        
        def normalize_day(day_str):
            return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', day_str)

        df['Day_Normalized'] = df['Day'].apply(normalize_day)
        df['Day_New'] = df['Day_Normalized'] +" - scraper_run"
        df['start_datetime'] = pd.to_datetime(df['Day_Normalized'] + ' ' + df['Start'], format='%d %b %Y %H:%M:%S')
        df['finish_datetime'] = pd.to_datetime(df['Day_Normalized'] + ' ' + df['Finish'], format='%d %b %Y %H:%M:%S')
        
        generic_date = datetime(2000, 1, 1)
        df['start_plot'] = df['start_datetime'].apply(lambda dt: datetime.combine(generic_date, dt.time()))
        df['finish_plot'] = df['finish_datetime'].apply(lambda dt: datetime.combine(generic_date, dt.time()))
        
        df['Duration'] = df['finish_datetime'] - df['start_datetime']
        df['Duration_str'] = df['Duration'].apply(lambda x: str(x).split('days')[-1].strip())
        both=True
        table_names.append("scraper_run")
    if 'processing_log_test' in df1 and df1['processing_log_test'] and len(df1['processing_log_test'])!=0:
        data_present=True
        table_names.append("processing_log_test")
        df2 = df1['processing_log_test'][0]
        df2['TaskID'] = df2.index.astype(str)
        
        def normalize_day(day_str):
            return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', day_str)

        df2['Day_Normalized'] = df2['Day'].apply(normalize_day)
        df2['Day_New'] = df2['Day_Normalized'] +" - processing_log_test"
        df2['start_datetime'] = pd.to_datetime(df2['Day_Normalized'] + ' ' + df2['Start'], format='%d %b %Y %H:%M:%S')
        df2['finish_datetime'] = pd.to_datetime(df2['Day_Normalized'] + ' ' + df2['Finish'], format='%d %b %Y %H:%M:%S')
        
        generic_date = datetime(2000, 1, 1)
        df2['start_plot'] = df2['start_datetime'].apply(lambda dt: datetime.combine(generic_date, dt.time()))
        df2['finish_plot'] = df2['finish_datetime'].apply(lambda dt: datetime.combine(generic_date, dt.time()))
        
        df2['Duration'] = df2['finish_datetime'] - df2['start_datetime']
        df2['Duration_str'] = df2['Duration'].apply(lambda x: str(x).split('days')[-1].strip())
        if both:
            df = pd.concat([df,df2])
        else:
            df = df2
    
    if data_present:
    
        df['sort_key_date'] = pd.to_datetime(df['Day_New'].str.split(' - ').str[0], format='%d %b %Y')
            
        # 2. Create a text column to sort by name ('scraper_run' vs 'processing_log_test')
        df['sort_key_text'] = df['Day_New'].str.split(' - ').str[1]
        
        # 3. Apply the multi-level sort
        df.sort_values(by=['sort_key_date', 'sort_key_text'], ascending=[False, False], inplace=True)
        
        # 4. Drop the temporary helper columns
        df.drop(columns=['sort_key_date', 'sort_key_text'], inplace=True)

        full_y_axis_order = []
        for day in reversed(all_dates_in_range):
            for table in table_names:
                # formatted_date = day.strftime('%d %b %Y')
                formatted_date = f"{day.day} {day.strftime('%b %Y')}"
                # print(formatted_date)
                full_y_axis_order.append(f"{formatted_date} - {table}")
        # print(df['Day_New'])

        valid_dates = {item.split(' - ')[0] for item in full_y_axis_order}
        df = df[df['Day_Normalized'].isin(valid_dates)]

        if not df.empty:
            existing_days = set(df['Day_New'].unique())
        else:
            existing_days = set()
        
        missing_days = [day for day in full_y_axis_order if day not in existing_days]
        if missing_days:
            generic_date = datetime(2000, 1, 1)
            placeholder_data = {
                'Day_New': missing_days,
                'start_plot': generic_date,       # Use NaT (Not a Time) for datetime columns
                'finish_plot': generic_date,
                'TaskID': 'placeholder',
                'Start': '',                # Use empty strings for custom data
                'Finish': '',
                'Duration_str': '',
                'Operator_site_id': None
            }
            placeholders_df = pd.DataFrame(placeholder_data)
            
            # 4. Add the placeholders to the main DataFrame
            df = pd.concat([df, placeholders_df], ignore_index=True)

        color_map = {
            '1': 'green',
            '0': 'red',
            '-1': 'white'
        }

        print(df['Status'].unique())
        status_map = {
            '0':'Failed_Scraper',
            '1':'Success',
            '-1':'Failed_Processing'
        }
        df['Status_text'] = df['Status'].map(status_map) 
        print(df['Status_text'])

        if not df.empty:
            fig = px.timeline(
                df,
                x_start="start_plot", 
                x_end="finish_plot", 
                y="Day_New",
                color="Status",
                color_discrete_map=color_map,
                category_orders={"Day_New": full_y_axis_order},
                custom_data=['Start', 'Finish', 'Duration_str', 'Operator_site_id', 'Status_text'],
                title="Daily Script Run-Time",
                range_x=[datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 1, 1, 23, 59, 59)]
            )

            fig.update_traces(hovertemplate='<b>Day:</b> %{y}<br>' +
                                        '<b>Start:</b> %{customdata[0]}<br>' +
                                        '<b>Finish:</b> %{customdata[1]}<br>' +
                                        '<b>Duration:</b> %{customdata[2]}<br>'+
                                        '<b>Status:</b> %{customdata[4]}<br>'+
                                        '<b>Operator_site_id:</b> %{customdata[3]}<extra></extra>')
            fig.update_layout(
                title_font_size=24,
                xaxis_title="Time of Day (24-Hour)",
                yaxis_title="Date",
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis_rangeslider_visible=False
            )
            fig.update_xaxes(
                tickformat='%H:%M', 
            )


            config = {
                'displayModeBar': True
            }
            st.plotly_chart(fig, use_container_width=True, config=config)

def format_duration(seconds):
    """Converts a given number of seconds into a string format of hr, min, s."""
    
    # Return immediately if the input is 0
    if seconds == 0:
        return "0s"
    
    # Make sure we're working with an integer
    seconds = int(seconds)
    
    # Calculate hours, minutes, and remaining seconds
    hours = seconds // 3600  # 1 hour = 3600 seconds
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    # Build a list of the parts of the time string that are not zero
    time_parts = []
    if hours > 0:
        time_parts.append(f"{hours}hr")
    if minutes > 0:
        time_parts.append(f"{minutes}min")
    if secs > 0:
        time_parts.append(f"{secs}s")
        
    # Join the parts with a space and return the result
    return " ".join(time_parts)

if __name__ == "__main__":
    # 1. Display the widgets and get the current selections from the user
    user_inputs = display_controls()
    
    # # 2. Add a button to trigger the chart generation
    # if st.button("Generate Chart 🚀"):
    #     # 3. Ensure we have valid inputs before proceeding
    if user_inputs:
        start_day, end_day, selected_tables, server_choice = user_inputs
        
        # 4. Fetch the data using the selected inputs
        with st.spinner("Fetching data and building chart..."):

            # start_time = time.time()
            processed_df = get_data(start_day, end_day, selected_tables, server_choice)
            # start_time = time.time() - start_time
            # st.write(f"Fetching + Processing: {start_time}s")
            # 5. Display the chart with the new data
            # start_time = time.time()

            # dropdown_options = ["All Days"] + list(usage.keys())
            # selected_day = st.selectbox(
            #     "Choose a day to display:",
            #     options=dropdown_options
            # )
            for table_name_dis in ['scraper_run',"processing_log_test"]:
                if table_name_dis in processed_df and processed_df[table_name_dis]:
                    st.header(f"Displaying {table_name_dis} metrics from {start_day.strftime('%b %d, %Y')} to {end_day.strftime('%b %d, %Y')}")
                    usage = processed_df[table_name_dis][1]
                    count = processed_df[table_name_dis][2]
                    days = len(usage)
                    summary_data = dict()
                    total_scripts = 0
                    total_time = 0
                    total_per = 0
                    for i,val in usage.items():
                        total_scripts += count.get(i)
                        total_time += (86400 - len(val)) 
                        total_per += (len(val)/86400 * 100)
                        # summary_data[i] = {
                        #     'Total Scripts Run:': count.get(i),
                        #     'Idle Time:': format_duration(86400 - len(val)),
                        #     'Server Utilization Percent': len(val)/86400 * 100
                        # }
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            label="Average Idle Time",
                            value=format_duration(total_time/days),
                        )

                    with col2:
                        st.metric(
                            label="Total Scripts Run",
                            value=f"{total_scripts:,}", # Format with a comma
                        )

                    with col3:
                        # Format the float to two decimal places for a cleaner look
                        formatted_utilization = f"{total_per/days:.2f}%"
                        st.metric(
                            label="Average Server Utilization",
                            value=formatted_utilization,
                        )
            display_chart(processed_df, start_day, end_day)
            # start_time = time.time() - start_time
            # st.write(f"Display: {start_time}s")