# coalesce-streamlit-lab
Coalesce+Streamlit Hands-on Lab

### Prerequisite ###

To run this Streamlit, the following criteria must be met: 

- Your Snowflake account must be hosted on AWS (Streamlit-in-Snowflake is only available on AWS as of now).
- You must have completed the Coalesce Hands-On Guide, which can be accessed here: https://guides.coalesce.io/foundations/index.html#0

### Instructions ###
Once the Coalesce Hands-on Guide is completed, you should have a database with your chosen name and two schemas (`WORKDEV` and `WORKTEST`). This application will require the following tables to be available and populated with data: 

- `FCT_LINEITEM_ORDERS_DIM_LOOKUP`
- `DIM_SUPPLIER`
- `DIM_PART`

Streamlit apps can be deployed in different ways. The focus of this hands-on lab is to leverage Snowflake's built-in UI, which doesn't require a Python environment to be set up on the user's computer. 

In order to deploy the Streamlit app (Open file `app.py` for the source code), first, log into your Snowflake account by navigating to its dedicated UI. Make sure you use the same Snowflake account that you have used for the Coalese Hands-on Guide earlier.

Once logged in, make sure your current role is set to `ACCOUNT_ADMIN`. Once selected, you should see the <strong>Streamlit</strong> link on your Snowflake's home screen:

![Alt text](/images/01.jpg)

Once selected, click on the <strong>+ Streamlit App</strong> button on the top right hand corner:

![Alt text](/images/02.jpg)

In the pop-up form, give a friendly name to your Streamlit app. Since the Streamlit app requires compute to run, you need to nominate a virtual warehouse to power the application. You also need to provide the database and schema where the app needs to be hosted. Each Streamlit app is a native Snowflake object, and therefore creating them in the right schema and database can simplify access control. For this tutorial, we recommend you save the app in the same database and schema where the Coalesce lab's output dataset is stored. The following screenshot shows how these parameters can be set:

![Alt text](/images/03.jpg)

Once you click on the <strong>Create</strong> button, you should be redirected to Snowflake's built-in UI for building Streamlit apps. The UI comes with a sample Streamlit app. After a few seconds, the executable app should be available on the right hand side:

![Alt text](/images/04.jpg)

There are three small icons at the bottom left hand side of the screen that can be used to show/hide Snowflake's object explorer, the source code window, and the preview panel. 

In order to run the Hands-on Lab's Streamlit app, open the source file, copy the content and paste them into the code editors in the UI.

Once done, find the following lines in the code and make sure these variables are pointing to the right tables:

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

Once checked, click on the <strong>Run</strong> button on the top right hand corner of the screen. This would pick up the pasted code and the app should refresh within a few seconds. You should see a screen similar to the below screenshot:

![Alt text](/images/05.jpg)

The virtual warehouse powering the app will be in active state for as long as the app is being used. Not using the app will result in suspension of the app and consequently the virtual warehouse. Clicking back on the preview pane or clicking the Run button will launch the app again. 

To modify the warehouse in use or to delete the app, you can click on the name of the app on the top left corner of the screen and choose the option from the pop-up menu:

![Alt text](/images/06.jpg)

