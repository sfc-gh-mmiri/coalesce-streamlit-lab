# Import python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, count, sum, when, lit, date_part, concat, lpad, trunc


# Establishing a Snowpark session using the existing connection
session = get_active_session()


# Source table names 
# (Use fully qualified names when referencing different databases and/or schemas.)
param_fact_name = "FCT_LINEITEM_ORDERS_DIM_LOOKUP"
param_dim_supplier_name = "DIM_SUPPLIER"
param_dim_part_name = "DIM_PART"


# Source tables (Snowpark dataframes)
# Dimensions are filtered to active rows only
fact_lineitem_orders = session.table(param_fact_name)
dim_supplier = session.table(param_dim_supplier_name).filter(col("SYSTEM_CURRENT_FLAG")=='Y')
dim_part = session.table(param_dim_part_name).filter(col("SYSTEM_CURRENT_FLAG")=='Y')


# Returns a unique list of part types as a pandas dataframe (to be used as a UI filter)
@st.cache_data
def get_unique_part_types():
    return dim_part.select("P_TYPE").distinct().order_by(col("P_TYPE")).to_pandas()


# Returns a unique list of part brands as a pandas dataframe (to be used as a UI filter)
@st.cache_data
def get_unique_brands():
    return dim_part.select("P_BRAND").distinct().order_by(col("P_BRAND")).to_pandas()


# Returns a unique list of supplier names as a pandas dataframe (to be used as a UI filter)
@st.cache_data
def get_unique_suppliers():
    return dim_supplier.select("S_NAME").distinct().order_by(col("S_NAME")).to_pandas()


# Returns the min/max shipping dates as a pandas dataframe (to be used as a UI filter)
@st.cache_data
def get_date_ranges():
    return session.sql("""
    SELECT 
        MIN(LINEITEM_SHIPDATE) min_date,
        MAX(LINEITEM_SHIPDATE) max_date
    FROM {0}""".format(param_fact_name)).to_pandas()


# Returns a Snowpark dataframe object pointing to the curated list of orders 
# & line items with all the filters applied and the two dimensions joined
def get_order_details(filter_part_type,filter_brand_name,filter_supplier_name,filter_shipping_date):

    # Applying the selected filters

    # If len(...)>0 it means the user has selected at least one value
    # and the results need to be filtered (to be executed as an IN statement in SQL)
    if len(filter_part_type)>0:
        dim_part_filtered = dim_part[dim_part['P_TYPE'].isin(filter_part_type)]
    # Otherwise, do not apply any filters
    else:
        dim_part_filtered = dim_part
    
    if len(filter_brand_name)>0:
        dim_part_filtered = dim_part_filtered[dim_part_filtered['P_BRAND'].isin(filter_brand_name)]
    
    if len(filter_supplier_name)>0:
        dim_supplier_filtered = dim_supplier[dim_supplier['S_NAME'].isin(filter_supplier_name)]
    else:
        dim_supplier_filtered = dim_supplier

    # Additional column: Order Status - Order Year-Month - Order Priority
    # Order Status is going to be converted to a CASE statement in SQL
    # The other two columns are concatenated values derived from other columns
    lineitem_orders_filtered = fact_lineitem_orders\
        .with_column('"Order Status"',when(col("ORDER_ORDERSTATUS")=='O','Not shipped yet')\
            .when(col("ORDER_ORDERSTATUS")=='P','Partially shipped')\
            .when(col("ORDER_ORDERSTATUS")=='F','Fully shipped')\
            .when(col("ORDER_ORDERSTATUS")=='C','Cancelled')\
            .otherwise('Unknown'))\
        .with_column('"Order Year-Month"',\
                concat(date_part('yyyy',col("ORDER_ORDERDATE")),lit('-'),\
                lpad(date_part('mm',col("ORDER_ORDERDATE")),2,lit('0'))))\
        .with_column("Order Priority",\
                concat(col("ORDER_ORDERPRIORITY_NUM"),lit('-'),col("ORDER_ORDERPRIORITY_DESC")))

    # Joining the modified fact (with the added columns) to the filtered dimensions 
    # Another filter is applied to only return data for the date range selected by the user
    lineitem_orders_filtered = lineitem_orders_filtered\
    .join(dim_part_filtered,
    dim_part_filtered["DIM_PART_KEY"]==lineitem_orders_filtered["DIM_PART_KEY"])\
    .join(dim_supplier_filtered,
    dim_supplier_filtered["DIM_SUPPLIER_KEY"]==lineitem_orders_filtered["DIM_SUPPLIER_KEY"])\
    .filter((col("LINEITEM_SHIPDATE")>=filter_shipping_date[0]) & (col("LINEITEM_SHIPDATE")<=filter_shipping_date[1]))\
    .select(
        col("O_CUSTKEY").as_("Customer Key"),
        col("LINEITEM_ORDERKEY").as_("Order Key"),
        col("LINEITEM_LINENUMBER").as_("Line Number"),
        col("LINEITEM_QUANTITY").as_('"Quantity"'),
        col("LINEITEM_EXTENDEDPRICE").as_("Extended Price"),
        col("LINEITEM_DISCOUNT").as_('"Discount"'),
        col("LINEITEM_TAX").as_('"Tax"'),        
        col("LINEITEM_LINESTATUS").as_("Line Status"),
        col("LINEITEM_SHIPDATE").as_("Ship Date"),
        col("LINEITEM_COMMITDATE").as_("Commit Date"),
        col("LINEITEM_RECEIPTDATE").as_("Receipt Date"),
        col("LINEITEM_SHIPINSTRUCT").as_("Shipping Instructions"),
        col("LINEITEM_SHIPMODE").as_("Shipping Mode"),                
        col("ORDER_TOTALPRICE").as_("Total Price"),
        col("ORDER_ORDERDATE").as_("Order Date"),
        col("ORDER_ORDERPRIORITY_NUM").as_("Priority Code"), 
        col("ORDER_ORDERPRIORITY_DESC").as_("Priority Description"),
        col("ORDER_SHIPPRIORITY").as_("Shipping Priority"),
        col("DAYS TO SHIP").as_("Days to Ship"),
        col("PS_AVAILQTY").as_("Available Quantity"),
        col("PS_SUPPLYCOST").as_("Supply Cost"),
        col("S_NAME").as_("Supplier Name"),
        col("S_ACCTBAL").as_("Supplier Account Balance"),
        col("P_PARTKEY").as_("Part Key"),
        col("P_NAME").as_("Part Name"),
        col("P_BRAND").as_("Part Brand"),
        col("P_MFGR").as_('"Manufacturer"'),        
        col("P_TYPE").as_("Part Type"),
        col("P_SIZE").as_("Part Size"),
        col("P_CONTAINER").as_("Part Container"),
        col("P_RETAILPRICE").as_("Retail Price"),
        col("Order Status"),
        col("Order Priority"),
        col("Order Year-Month")
    )
    return lineitem_orders_filtered


# Returns a pandas dataframe with the summary of orders with delays 
# (where the commit date could not have been met)
def get_delayed_orders(lineitem_orders_filtered):    
    # Two conditional columns are added to be used later:
    # For counting orders with delays and all orders
    delayed_orders = lineitem_orders_filtered\
        .with_column('"Late Received"',when(col("Ship Date")>col("Commit Date"),1)\
                     .when(col("Receipt Date")>col("Commit Date"),1)\
                     .otherwise(0))\
        .with_column('"Sent"',lit(1))

    delayed_orders = delayed_orders\
        .select(col("Order Priority"),
                col('"Sent"'),                
                col("Late Received"),
                col("Receipt Date"),
                col("Ship Date"),
                col("Commit Date"))\
        .group_by(col("Order Priority"))\
        .agg((sum(col('"Sent"'))).alias('"Sent Items"')\
             ,(sum(col("Late Received"))).alias("Late Received Items"))\
        .order_by(col("Order Priority"))

    # Additional calculated column for % of delayed orders
    delayed_orders = delayed_orders\
        .with_column ('% of Delayed Shipments',col("Late Received Items")/col("Sent Items"))\
        .to_pandas()
    
    # Resetting the index to start from 1 (instead of 0)
    delayed_orders.index = range(1,len(delayed_orders)+1)
    return delayed_orders


# Returns a pandas dataframe of the revenue generated from orders by day
def get_orders_by_day(lineitem_orders_filtered):
    return lineitem_orders_filtered\
    .group_by(col("Ship Date"))\
    .agg(count(col("Order Key")).alias("Count of Orders"),trunc(sum(col("Extended Price")*(1-col('"Discount"')))).alias("Total Revenue")).to_pandas()    


# Returns a pandas dataframe containing the count of orders by month
def get_orders_by_month(lineitem_orders_filtered):
    return lineitem_orders_filtered\
        .group_by(col("Order Year-Month"))\
        .agg(trunc(sum(col("Extended Price")*(1-col('"Discount"')))).alias("Total Revenue"),count(col("Order Key")).alias("Count of Orders"))\
        .order_by(col("Order Year-Month")).to_pandas()


# Returns the top/bottom N suppliers by revenue
# The number of records to return and the top/bottom part can be set via parameters
def get_top_n_suppliers(filter_limit,ascending):
    suppliers = lineitem_orders_filtered\
        .group_by(col("Supplier Name"))\
        .agg(sum(col("Extended Price")*(1-col('"Discount"'))).alias("Total Revenue"))\
        .order_by("Total Revenue",ascending=ascending).limit(filter_limit).to_pandas()
    suppliers.index = range(1,len(suppliers)+1)
    return suppliers


# Suppliers with the highest number of unshipped products
def get_suppliers_with_unshipped_orders(lineitem_orders_filtered,filter_limit):
    return lineitem_orders_filtered\
    .filter(col("Order Status")!='Fully shipped')\
    .group_by(col("Supplier Name"))\
    .agg(count(col("Order Key")).alias("Count of Orders"))\
    .order_by("Count of Orders",ascending=False)\
    .limit(filter_limit).to_pandas()

# ------------------------------
# Main body of the Streamlit App
# ------------------------------
st.title(":chart_with_upwards_trend: Hands-on-Lab Streamlit App")
st.write('This application points to the output from the Coalesce Hands-on Lab. \n\
    Use the controls below to filter and limit the data points.')


# Filters => Part Type - Part Brand Name - Supplier Name - Ship Date
# All filters are multi-select, so the return variables are of "list" type
filter_part_type = st.multiselect('Part Type:',get_unique_part_types())
filter_brand_name = st.multiselect('Brand Name:',get_unique_brands())
filter_supplier_name = st.multiselect('Supplier Name:',get_unique_suppliers())
filter_shipping_date = st.slider('Ship Date:',value=
                                 (get_date_ranges().iloc[0,0],
                                  get_date_ranges().iloc[0,1]))


# Prepare the orders dataframe with all filters and joins applied
# This dataframe will be referenced multiple times later in the code
lineitem_orders_filtered = get_order_details(filter_part_type,
                                             filter_brand_name,
                                             filter_supplier_name,
                                             filter_shipping_date)


st.header('Delayed Orders')
st.write('The following shows the number of items that were shipped and/or received later\
    than the date commmitted to the customers.')

# Calling the function to get the aggregated results in a pandas dataframe
delayed_orders = get_delayed_orders(lineitem_orders_filtered)

# Showing the returned results in an interactive table in the UI
st.dataframe(delayed_orders,use_container_width=True)

st.write('---')

st.header('Order Count by Day')


# Order Revenue by day
orders_by_day = get_orders_by_day(lineitem_orders_filtered)
# Streamlit's built-in line chart component
st.line_chart(orders_by_day,x='Ship Date',y='Count of Orders',use_container_width=True)


st.header('Monthly Breakdown')
# A user control for choosing what chart (or charts) to display
measure_name = st.radio('Measure:',['Count of Orders','Total Revenue','Both'],
                        horizontal=True,index=0)


# Monthly orders
orders_by_month = get_orders_by_month(lineitem_orders_filtered)

# Bar chart = Monthly order count
monthly_orders_chart = alt.Chart(orders_by_month).mark_bar().encode(
    x=alt.X('Order Year-Month:N'),
    y=alt.Y('Count of Orders:Q'),
    opacity=alt.value(0.80),
    color=alt.value("#4682B4")
)    

# Line chart = Monthly revenue
monthly_revenue_chart = alt.Chart(orders_by_month).mark_line().encode(
    x=alt.X('Order Year-Month:N'),
    y=alt.Y('Total Revenue:Q'),
    color=alt.value("#E91E63")
)


# Based on what measure has been selected by the user, the right chart will be shown
# If 'Both' is selected, they will be combined into one chart
if measure_name=='Both':
    monthly_breakdown_chart = alt.layer(monthly_orders_chart,monthly_revenue_chart)\
        .resolve_scale(y='independent')
elif measure_name=='Count of Orders':
    monthly_breakdown_chart = monthly_orders_chart
else:
    monthly_breakdown_chart = monthly_revenue_chart

# Streamlit component for rendering altair charts in the UI        
st.altair_chart(monthly_breakdown_chart,use_container_width=True)


st.write('---')
st.header('Best/Worst Performing Suppliers')
# Filter for limiting the number of returned rows
filter_limit = st.radio('Row Limit:',[5,10,20,50],horizontal=True)


# Order Revenue by day
top_suppliers_with_unshipped_orders = get_suppliers_with_unshipped_orders(
    lineitem_orders_filtered,
    filter_limit)

# Horizonal bar chart for showing top suppliers with fulfilment issues
chart_top_suppliers_with_unshipped_orders = alt.Chart(top_suppliers_with_unshipped_orders)\
    .mark_bar().encode(
    x=alt.X('Count of Orders:Q'),
    y=alt.Y('Supplier Name:N',sort='-x'), 
    color=alt.value("#800000")    
).configure_axis(
    labelLimit=500
)

st.subheader('Suppliers with the Highest Fulfilment Issues')
st.altair_chart(chart_top_suppliers_with_unshipped_orders,use_container_width=True)

# Creating two columns in the UI to place the following tables side-by-side
col1,col2 = st.columns(2)
with col1:
    # Top N suppliers
    # Setting the 2nd argument to False returns the results 
    # in descending order (highest values on top)
    top_suppliers = get_top_n_suppliers(filter_limit,False)    
    st.subheader(':green[Top {0}] by Revenue'.format(filter_limit))
    st.dataframe(top_suppliers,use_container_width=True)

with col2:
    # Bottom N suppliers
    # Setting the 2nd argument to True returns the results 
    # in ascending order (lowest values on top)
    bottom_suppliers = get_top_n_suppliers(filter_limit,True)    
    st.subheader(':red[Bottom {0}] by Revenue'.format(filter_limit))
    st.dataframe(bottom_suppliers,use_container_width=True)

# Saving the results as a view
with st.expander('Click to Save...'):
    st.write('Enter the following details to materialise the underlying query as a view:')
    view_name = st.text_input('View Name:')
    comment = st.text_input('Comment:')
    
    create_view_button = st.button('Create View')
        
    # If the button has been clicked by the user
    if create_view_button:        
        # Materialize the filtered dataframe (with all the joins, etc.) as a view
        lineitem_orders_filtered.create_or_replace_view(view_name)
        # Altering the view to add the comment value
        session.sql("ALTER VIEW {0} SET COMMENT = '{1}'".format(view_name,comment)).collect()
        st.write('View {0} was created.'.format(view_name))
        