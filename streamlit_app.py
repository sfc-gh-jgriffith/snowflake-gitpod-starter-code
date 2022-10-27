import os
import streamlit as st
import snowflake.connector  #upm package(snowflake-connector-python==2.7.0)
import pandas as pd
from time import sleep

import seaborn as sns
import matplotlib.pyplot as plt
 
 
# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    con = snowflake.connector.connect(
        user=os.getenv("SFUSER"),
        password=os.getenv("PASSWORD"),
        account=os.getenv("ACCOUNT"),
        role=os.getenv("ROLE"),
        warehouse=os.getenv("WAREHOUSE")
    )
    return con
 
conn = init_connection()




@st.experimental_singleton
def create_results_history_array():
    return []
results_history = create_results_history_array()

platform = st.sidebar.selectbox("Platform", ['Snowflake','Databricks', 'Synapse', 'Bigquery'])


if platform=="Snowflake":
    query_schema = 'snowflake_sample_data.tpch_sf100'
elif platform=="Databricks":
    query_schema = 'snowmeter_demo.demo_data'

#main_screen = st.selectbox("Screen", ["Test Setup","Results", "Results History"])

query_tab, results_tab, history_tab = st.tabs(["Test Setup","Results", "Results History"])

# Perform query, using st.experimental_memo to only rerun when the query changes or after 10 min.
#@st.experimental_memo(ttl=60)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def run_query_pandas(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()



warehouse_sizes = ['X-Small','Small','Medium','Large','X-Large', '2X-Large']
warehouse_multicluster_range = (1,10)



def get_warehouses(): 
    return run_query("SHOW WAREHOUSES")

def get_warehouse_names():
    return [r[0] for r in get_warehouses()]


def get_wh_settings( wh_setting):
    current_warehouse = run_query('select current_warehouse()')[0][0]
    warehouse = [r for r in get_warehouses() if r[0] == current_warehouse][0]
    wh_settings = {'warehouse_name': 0,
                   'state': 1, 
                   'warehouse_size': 3,
                   'min_cluster_count': 4,
                   'max_cluster_count': 5,
                   'auto_suspend_time': 11
                  }

    return warehouse[wh_settings[wh_setting]]

def set_state():
    st.session_state.warehouse_name = get_wh_settings('warehouse_name')
    st.session_state.state = get_wh_settings('state')
    st.session_state.auto_suspend_time = get_wh_settings('auto_suspend_time')
    st.session_state.warehouse_size = get_wh_settings('warehouse_size')
    st.session_state.warehouse_cluster_count_range = (get_wh_settings('min_cluster_count'),
                                        get_wh_settings('max_cluster_count'))


def use_warehouse():
    run_query(f"use warehouse {st.session_state.warehouse_name}")
    set_state()

set_state()


def update_wh_setting(warehouse_name, wh_setting):

    if wh_setting == 'warehouse_cluster_count_range':
        run_query(f"alter warehouse {st.session_state.warehouse_name} " +
                f"set min_cluster_count = {st.session_state[wh_setting][0]}, " +
                f"max_cluster_count = {st.session_state[wh_setting][1]} " )
    else: 
        run_query(f"alter warehouse {st.session_state.warehouse_name} " +
                f"set {wh_setting} = '{st.session_state[wh_setting]}' " +
                f"wait_for_completion=TRUE")
    
    set_state()

def monitor_queries(qids):
    query_states = {q: conn.get_query_status(q) for q in qids}

    results_tab.write('Waiting for query completion')
    progress = results_tab.progress(0)

    def pct_done():
        return 1 - sum([int(conn.is_still_running(s)) for s in query_states.values()]) / len(qids)

    query_states_display = results_tab.empty()

    progress.progress(pct_done() )
    while(any([conn.is_still_running(s) for s in query_states.values()])):
        query_states = {q: conn.get_query_status(q) for q in qids}
        query_states_display.dataframe([str(v) for v in query_states.values()])
        progress.progress( pct_done() ) 

    progress.progress(pct_done()) 

    df_query_history = run_query_pandas("select * from table(snowflake.information_schema.query_history_by_session()  ) "+
                                 'where query_id in (' +
                                  ','.join(['\''+q+'\'' for q in qids]) +
                                  ')'
                                )
    elapsed_time = df_query_history['END_TIME'].max() - df_query_history['START_TIME'].min()
    results_history.append({
                        'platform': platform,
                        'warehouse_size': st.session_state.warehouse_size,
                        'min_cluster_count': st.session_state.warehouse_cluster_count_range[0],
                        'max_cluster_count': st.session_state.warehouse_cluster_count_range[1],
                        'queries_submitted': len(qids),
                        'batch_query_history': df_query_history.to_dict(),
                        'elapsed_time': elapsed_time.seconds
                    })

def submit_query_form():
    qids = []

    sql_statement = st.session_state.sql_statement
    number_of_queries = st.session_state.number_of_queries
    seconds_between_queries = st.session_state.seconds_between_queries
    
    query_tab.write('Submitting Queries')
    progress = query_tab.progress(0)

    with conn.cursor() as cur:
        cur.execute(f'use warehouse {st.session_state.warehouse_name}')
        cur.execute('alter session set use_cached_result = false')
    for i in range(number_of_queries):
        with conn.cursor() as cur: 
            cur.execute_async(sql_statement)
            qids.append(cur.sfqid)

        progress.progress((i+1) / number_of_queries)
        sleep(seconds_between_queries)
    
    
  
    monitor_queries(qids)
    set_state()

selected_wh = st.sidebar.empty()
warehouse_size = st.sidebar.empty()
multicluster_factor = st.sidebar.empty()


def sidebar_snowflake():
    st.sidebar.header('Snowflake Configuration')
    selected_wh.selectbox("Warehouse", get_warehouse_names(), on_change=use_warehouse, key="warehouse_name")
    warehouse_size.selectbox("Warehouse Size", 
                                      warehouse_sizes, 
                                      on_change = update_wh_setting,
                                      args=(selected_wh, 'warehouse_size'),
                                      key="warehouse_size")
    multicluster_factor.slider("Multi Cluster Factor",
                                            warehouse_multicluster_range[0], 
                                            warehouse_multicluster_range[1],
                                         on_change = update_wh_setting,
                                         args=(selected_wh, 'warehouse_cluster_count_range'),
                                         key="warehouse_cluster_count_range"
                                        )
    st.sidebar.text(f"Warehouse State: {st.session_state.state}")
    st.sidebar.text(f"Warehouse Auto Suspend Time: {st.session_state.auto_suspend_time}")
    st.sidebar.text(f"Current Role: {run_query('select current_role()')}")

def sidebar_databricks():
    st.sidebar.header('Databricks Configuration')
    selected_cluster = st.sidebar.selectbox("Cluster", ['Slow Cluster', 'Slower Cluster'])

st.session_state.sql_statement = f"""select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
  from
       {query_schema}.lineitem
       inner join {query_schema}.orders on lineitem.l_orderkey = orders.o_orderkey
 where
       o_orderdate <= dateadd(year, -2, to_date('1998-12-01'))
 group by
       l_returnflag,
       l_linestatus
 order by
       l_returnflag,
       l_linestatus;"""

st.session_state.number_of_queries = 1
st.session_state.seconds_between_queries = 0


if (platform == 'Snowflake'):
    sidebar_snowflake()
elif (platform == 'Databricks'):
    sidebar_databricks()



with query_tab:
    query_form = st.form(key="query_form")
    sql_statement = query_form.text_area("SQL Statement:", key="sql_statement")
    qty_queries = query_form.number_input("Number of Queries to Submit:", min_value=1, max_value=100, key="number_of_queries")
    seconds_between = query_form.number_input("Seconds between queries:", min_value=0, max_value=10, key="seconds_between_queries")

    submitted = query_form.form_submit_button("Submit", on_click=submit_query_form) 

with history_tab:
    if len(results_history):
        st.dataframe(pd.DataFrame(results_history)[['platform','queries_submitted','warehouse_size','min_cluster_count','max_cluster_count','elapsed_time']])

with results_tab:
    if len(results_history):
        df_query_history = pd.DataFrame(results_history[-1]['batch_query_history'])
        df_query_history.sort_values('START_TIME', ascending=True, inplace=True)
        df_query_history.reset_index(inplace=True, drop=True)
        df_query_history.reset_index(inplace=True)
        df_query_history['TOTAL_ELAPSED_TIME'] = df_query_history['TOTAL_ELAPSED_TIME'].astype(int)
        df_query_history['QUEUED_OVERLOAD_TIME'] = df_query_history['QUEUED_OVERLOAD_TIME'].astype(int)
        df_query_history['EXECUTION_TIME'] = df_query_history['EXECUTION_TIME'].astype(int)
        df_query_history['CLUSTER_NUMBER'] = df_query_history['CLUSTER_NUMBER'].fillna(1)

        st.header("Total Time Elapsed by Query")
        st.bar_chart(df_query_history, x='index', y='TOTAL_ELAPSED_TIME')
        st.header("Cluster Used by Query")
        cluster_heatmap_data = df_query_history.assign(used_cluster='X').pivot(index='index', columns='CLUSTER_NUMBER', values='used_cluster').fillna(' ')
        st.dataframe(cluster_heatmap_data.T)
        
        st.header("Queued Time by Query")
        st.bar_chart(df_query_history, x='index', y='QUEUED_OVERLOAD_TIME')
        st.header("Execution Time by Query")
        st.bar_chart(df_query_history, x='index', y='EXECUTION_TIME')
        st.header("Test Details")
        st.dataframe(df_query_history)