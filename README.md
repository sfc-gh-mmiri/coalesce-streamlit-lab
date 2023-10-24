# coalesce-streamlit-lab
Coalesce+Streamlit Hands-on Lab

### Prerequisite ###

In orde to run this Streamlit lab, the following criteria must be met: 

- Your Snowflake account must be hosted on AWS, as Streamlit-in-Snowflake is currently available on Snowflake accounts hosted on AWS only.
- You must have completed the Coalesce Hands-On Guide, which can be found at this link: https://guides.coalesce.io/foundations/index.html#0

### Instructions ###
After finishing the Coalesce Hands-on Guide, you should have a database named of your choice along with two schemas (`WORKDEV`` and `WORKTEST``). This Streamlit app will need the following tables to be present and filled with data:

- `FCT_LINEITEM_ORDERS_DIM_LOOKUP`
- `DIM_SUPPLIER`
- `DIM_PART`

Streamlit apps can be deployed in multiple ways. In this hands-on lab, we'll be utilising Snowflake's built-in UI, which doesn't require a Python environment to be set up on the user's computer.

In order to deploy the Streamlit app (Source code can be accessed by opening `app.py`), first, log into your Snowflake account's Snowsight UI. Make sure you're using the same Snowflake account that you have used for the Coalese Hands-on Guide earlier.

Once logged in, make sure your current role is set to `ACCOUNT_ADMIN`. Once selected, you will find the <strong>Streamlit</strong> link on your Snowflake's home screen:

![Alt text](/images/01.jpg)

Once selected, click on the <strong>+ Streamlit App</strong> button on the top right-hand corner:

![Alt text](/images/02.jpg)

In the pop-up form, enter a friendly name for your Streamlit app. Since the Streamlit app runs on Snowflake compute, you need to nominate a virtual warehouse for the app to use. You also need to specify the database and schema where the app should be deployed to. Streamlit apps are native Snowflake objects, and therefore creating them in the right schema and database is important for proper access management. In this tutorial, we recommend using the same database and schema where the Coalesce lab's output dataset resides. The following screenshot shows how to configure these parameters:

![Alt text](/images/03.jpg)

Once you click on the <strong>Create</strong> button, you'll be redirected to Snowflake's built-in UI for building Streamlit apps. The UI is preloaded with a sample Streamlit app code. After a few seconds, the executable app should be running on the right-hand side:

![Alt text](/images/04.jpg)

At the bottom left corner of the screen, you'll find three small icons for showing or hiding Snowflake's object explorer, the source code window, and the preview panel.

To execute the Hands-on Lab's Streamlit app, open the source file, copy the content and paste into the code editor in the UI.

Once the code is pasted, locate the following lines in the code and make sure these variables are pointing to the right tables:

```
param_fact_name = "FCT_LINEITEM_ORDERS_DIM_LOOKUP"
param_dim_supplier_name = "DIM_SUPPLIER"
param_dim_part_name = "DIM_PART"
```

If the Streamlit app is hosted on a different schema and/or database from where the tables are located, you need to use fully qualified names, such as :

```
param_fact_name = "HOL_DB.WORKDIR.FCT_LINEITEM_ORDERS_DIM_LOOKUP"
param_dim_supplier_name = "HOL_DB.WORKDIR.DIM_SUPPLIER"
param_dim_part_name = "HOL_DB.WORKDIR.DIM_PART"
```

Once checked, click on the <strong>Run</strong> button on the top right-hand corner of the screen. This will spin up a new Python runtime environment on the nominated compute cluster, and after a few seconds, you should see the live app in working state. Your screen should look similar to the below screenshot:

![Alt text](/images/05.jpg)

The virtual warehouse powering the app will be in active state for as long as the app registers user interaction. If the app is left unused for a few minutes, it'll be suspended and the underlying warehouse will be paused after the set period of inactivity. Clicking the Run botton or anywhere in the preview panel will relaunch the app (which will automatically resume warehouse if paused).

To switch to a different virtual warehouse or to delete the app, you can click on the name of the app on the top left corner of the screen and choose the desired option from the pop-up menu:

![Alt text](/images/06.jpg)

