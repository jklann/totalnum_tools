import json
import sqlite3, pyodbc

import time
import keyring
import networkx as nx
import dash
import dash_auth
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State, MATCH, ALL
import dash_bootstrap_components as dbc

"""
Requires Dash. Recommended install:
1) Install Anaconda3
2) Install Dash: conda install -c conda-forge dash
3) Install dash-bootstrap: conda install -c conda-forge dash-bootstrap-components
4) Optional pyodbc to connect to mssql: conda install -c conda-forge pyodbc
5) pip install dash-auth
6) conda install tabulate
"""
# Experimental visualization of totalnum data using the output of totalnum_builddb. Uses dash.
# Now supports deployment, multiple sessions, and gunicorn!
# To run with gunicorn: GUNICORN_CMD_ARGS="--bind=0.0.0.0" gunicorn 'totalnum_dashboard_new:initApp("/path/to/database")'
# by Jeff Klann, PHD 10-2018 to 6-2020


instruction = """
1. Optionally choose a site from the dropdown
2. The options in the checkboxes start at the top level of the ontology. To navigate down the tree, check one box, click the right arrow button, and its children are displayed. Likewise, to navigate up the tree, click the left arrow button.
3. Graphs:
   * Top-left: Check boxes as desired. Temporal totalnum will appear below, showing the trend in # patients at each refresh.
   * Top-right: Check boxes as desired. Site-by-site totalnum will appear, showing the breakdown in # patients per site (max among all refreshes).
   * Bottom: Click left or right arrow as desired. Network graph of current ontology level and its children are displayed, with node size indicating the totalnum per item (at selected site, max among all refreshes).
"""
app = dash.Dash(external_stylesheets=[dbc.themes.CERULEAN],suppress_callback_exceptions=False)

# App Auth
# Set username and password in local install, instead of hello world
auth = dash_auth.BasicAuth(
    app,
    [['hello','world']]
)


# App Layout
app.layout = html.Div([
    html.Span(id='hoo'),
    dbc.NavbarSimple(children=[
        dbc.Button('Help', id='help',color='primary')
        ],brand="Patient Count Data Quality Dashboard",brand_href='#',brand_style={'align':'left'}),
    dbc.Modal(
        [
            dbc.ModalHeader("Totalnum Dashboard Help"),
            dbc.ModalBody(dcc.Markdown(instruction)),
            dbc.ModalFooter(
                #dbc.Button("Close", id="close", className="ml-auto")
            ),
        ],
        id="modalHelp",
        size='xl'
    ),
    dbc.Row(dbc.Col(dbc.RadioItems(id='site', options=[],inline=True),width=4),justify='center'),

    dbc.Row([
        dbc.Col(children=[dbc.Tabs([
            dbc.Tab(html.Div([
                dcc.Checklist(id='items', options=[{'label': 'No options', 'value': 'none'}], value=[],
                              labelStyle={'display': 'none'}),
                dbc.ButtonGroup([
                   dbc.Button("no options")
                ],vertical=True,id="navbuttons"),
                html.Br(),
                dbc.Button('<--', id='unzoom', outline=True, color='primary'),
                dbc.Button('-->', id='zoom', outline=True, color='primary')
            ],style={'width': '300px','margin':'20px'}),label='Navigate Terms')
        ])],width=2),

        dbc.Col(children=[dbc.Tabs([
            # Summary tab
            dbc.Tab(dbc.Card([dbc.CardHeader('',id='summary_head'),dbc.CardBody(dcc.Markdown('',id='summary'))],color='secondary',style={'width': '500px'}),label='Summary',tab_id='summary_tab',label_style={"color": "blue"}),
            # Explorer tab
            dbc.Tab(dbc.Table([html.Tr([
                html.Td(dbc.Tabs([
                    dbc.Tab(dcc.Graph(id='hlevel_graph'),label='Trends Over Time',tab_id='hlevel_tab',disabled=True),
                    dbc.Tab(dcc.Graph(id='bars_graph'),label='Trends Across Sites',tab_id='bars_tab',disabled=True)
                ],id='graphTabs'))] )
            ]),label="Explorer",tab_id='explorer_tab',label_style={"color": "blue"}),
            # Site variability tab
            dbc.Tab(dbc.Tabs([dbc.Tab(
                html.Div([dbc.Row([dbc.Col(dcc.Slider(id='slider_siteoutlier',min=0,max=4,step=0.1,value=1)),dbc.Col(html.P('Threshold',id='slidertext_siteoutlier'))]),dbc.Row([
                    #dbc.Col(dcc.Checklist(id='items_siteoutlier', options=[{'label': 'No options', 'value': 'none'}], value=[],
                    #              labelStyle={'display': 'block'}),width={"size":3,"offset":1}),
                    dbc.Col(dcc.Graph(id='siteoutlier_graph'),width=9)
                    ])
                ]),label='Explore'),
                dbc.Tab(html.Div("Variability Report (not yet implemented)",id='report_div'),label='Report')]
                ),label='Site Variability',tab_id='siteoutlier_tab',label_style={"color":"blue"}),
            # Missingness tab
            dbc.Tab(dbc.Tabs([
                # Missingness xplore
                dbc.Tab(dcc.Graph(id='missing_graph'),label='Explore',tab_id='missgraph_tab'),
                # Missingness report
                dbc.Tab([
                    dbc.Alert("All non-leaf items missing at this site, but not missing at least one other site. Red indicates missing, black just shows hierarchical structure."),
                    html.Div("# Missingness",id='missing_div'),
                    dbc.ListGroup([dbc.ListGroupItem(active=True)],id='missing')
                ],label='Report')
            ]),label="Missingness",tab_id='missing_tab',label_style={"color": "blue"})
        ],id='mainTabs',active_tab='summary_tab')],width=9)
    ]),

    #dcc.Graph(id='tree_graph'),
    html.Div('written by Jeffrey Klann, PhD'), html.Br(),
#    html.Div('errmsghere-todo-', id='msg0'),
    html.Div('app_state', id='app_state'),
    html.Br()
])

""" For SQLite, dbtype = "SQLITE" and db = filename of db
    For MSSQL, dbtype = "MSSQL" and db is pyodbc format: DSN=MYMSSQL;UID=myuser;PWD=mypassword
    For MSSQL, dbtype = "MSSQL" and db is a dict of server, user, password, and db (e.g., dbo)
"""
def initApp(*, dbtype="SQLITE",db="/Users/jklann/Google Drive/SCILHS Phase II/Committee, Cores, Panels/Informatics & Technology Core/totalnums/joined/totalnums.db"):
    global conn,app,dbstyle,sites
    # Initialize dashboard-wide globals
    if dbtype=="SQLITE":
        conn = sqlite3.connect(db,detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)  # Parameter converts datetimes
    elif dbtype=="MSSQL":
        conn = pyodbc.connect(db)
    # Store db type - let's call it db style
    dbstyle=dbtype
    print("Dash version:"+str(dcc.__version__))

    # Get site list and store it in a global
    sites = pd.read_sql("select distinct site from totalnums", conn).site.tolist()
    if 'All' not in sites: sites.append('All')
    options = list(map(lambda a: {'label': a, 'value': a}, sites))
    #options.append({'label':'All','value':'All'})
    app.layout['site'].options = options
    app.layout['site'].value = 'All'
    #app.layout['site'].children=[dbc.DropdownMenuItem(x) for x in sites]

    return app.server

# This callback just clears the checkboxes when the button is pressed, otherwise they are never cleared when the
# options are updated and hidden checkboxes accumulate in the state.
@ app.callback(
    Output('items', 'value'),
    [Input('zoom', 'n_clicks'), Input('unzoom', 'n_clicks')]
)
def clearTheChecks(clix, unclix):
    return []

# New callback to print help
@app.callback(
    Output('modalHelp','is_open'),
    [Input('help','n_clicks')],
    [State('modalHelp','is_open')]
)
def cbHelp(help,is_open):
    if help:
        return not is_open
    return is_open

# New callback - switch explorer tabs depending on site or All selected
@app.callback(
    Output('graphTabs','active_tab'),
    [Input('site', 'value')],
    [State('app_state', 'children')]
)
def cbSiteSwitchTab(site,app_state):
    if site=='All':
        return 'bars_tab'
    else:
        return 'hlevel_tab'

# New callback to print a summary header when a site is selected
@app.callback(
    Output('summary_head', 'children'),
    [Input('site','value')],
    [State('app_state','children')]
)
def cbSummaryHead(site,app_state):
    global conn
    if site is not None and site!='All':
        return site+' Summary'
    elif site=='All':
        return 'All sites selected'
    return ""


# New callback to print a summary when a site is selected
@app.callback(
    Output('summary', 'children'),
    [Input('site','value')],
    [State('app_state','children')]
)
def cbSummary(site,app_state):
    global conn
    if site is not None and site!='All':
        # TODO: This query is specific to COVID ontology and should reflect some principled way of determining elements for a summary table
        query = "select c_name, agg_count from totalnums_recent_joined where c_hlevel<6 and site='" + site + "'"
        dfsum = pd.read_sql_query(query, conn)
        return dfsum.to_markdown()
    elif site=="All":
        return "Select a single site for a summary."
    return ""

# New callback: update outlier options when the slider is changed
@app.callback(
    Output('slidertext_siteoutlier', 'children'),
    [Input('slider_siteoutlier','value')],
    [State('app_state','children')]
)
def cbSiteoutlierSliderText(slider,app_state):
    return "Threshold: " + str(slider)

# New callback: clear the active site selection when a tab is changed
@app.callback(
    Output('site', 'value'),
    [Input('mainTabs','active_tab')],
    [State('app_state','children'),State('site','value')]
)
def cbActiveTabSiteAdjustment(active_tab,app_state,already_site):
    print("ACTIVE" + already_site)
    return ""

# New callback: update outlier options when the slider is changed
"""@app.callback(
    Output('items_siteoutlier', 'options'),
    [Input('slider_siteoutlier','value'),Input('site','value')],
    [State('app_state','children')]
)"""
def cbSiteoutlierItems(slider,site,app_state):
    global conn
    c = conn.cursor()
    #appstatedict = json.loads(app_state)

    if site!='All':

        sql = "select c_fullname AS value, c_name AS label from outliers_sites o inner join bigfullname b on o.fullname_int=b.fullname_int where site='%s' and agg_count>average+(%s*stdev)" % (
            site,slider)
        #sql = "select distinct c_fullname AS value,c_name AS label from totalnums_oldcols t inner join bigfullname b on t.fullname_int=b.fullname_int where c_hlevel='%s' and site='%s' and c_fullname like '%s'" % (
        #    str(appstatedict['hlevel']), appstatedict['site'], '\\'.join(appstatedict['path']) + '\\%')
        items = pd.read_sql_query(sql, conn).to_dict('records')
        print(sql)
        return items

# New callback: display missingness markdown when a site is selected
@app.callback(
    Output('missing_div', 'children'),
    [Input('site','value')],
    [State('app_state','children')]
)
def cbMissingMd(site,app_state):
    global conn
    c = conn.cursor()
    # TODO: This is all non-leaf missingness. Probably want to look at leaf variance between sites vs. annotated high level missing
    query = """select c_fullname, c_tooltip, c_hlevel, c_name from bigfullname fn inner join totalnums_recent r on r.fullname_int=fn.fullname_int 
      where fn.fullname_int not in (select fullname_int from totalnums_recent where site='{{}}') and c_visualattributes not like 'L%' and c_visualattributes not like '_H%'
      order by c_hlevel """
    query=query.replace("{{}}",site)
    df = pd.read_sql_query(query,conn)
    df=df.sort_values(by='c_fullname',axis=0,ascending=True)
    # I had this on one line but was too hard to debug
    # Compute a readable string for missingness
    retval = []
    current = []
    for x in df.to_dict('records'):
        parensplit = x['c_tooltip'].split('\\') # Compute this here bc hlevel is unreliable
        if len(parensplit)>2:
            for c,s in enumerate(parensplit[2:],start=1):
                if s not in current:
                    txtcolor='red' if c==len(parensplit[2:]) else 'black'
                    retval.append(html.P(s,style={'margin-top':0,'margin-bottom':0,'padding':0,'margin-left':10*c,'color':('red' if c==len(parensplit[2:]) else 'black')}))
                    #retval.append(("#"*c)+' ' + str(c) + '.' + ('*'+s+'*' if c==len(parensplit[2:]) else s))
            current=parensplit
    return retval
    #return [dbc.ListGroupItem(x['c_tooltip'].split('\\')[2] + ":" + x['c_name']) if x['c_hlevel']>2 else  + ":" + x['c_name']) for x in df.to_dict('records')]


# New callback: display missingness when a site is selected
#@app.callback(
#    Output('missing', 'children'),
#    [Input('site','value')],
#    [State('app_state','children')]
#)
def cbMissing(site,app_state):
    global conn
    c = conn.cursor()
    # TODO: This is all non-leaf missingness. Probably want to look at leaf variance between sites vs. annotated high level missing
    query = """select c_tooltip, c_hlevel, c_name from bigfullname fn inner join totalnums_recent r on r.fullname_int=fn.fullname_int 
      where fn.fullname_int not in (select fullname_int from totalnums_recent where site='{{}}') and c_visualattributes not like 'L%' and c_visualattributes not like '_H%'
      order by c_hlevel """
    query=query.replace("{{}}",site)
    df = pd.read_sql_query(query,conn)
    # I had this on one line but was too hard to debug
    # Compute a readable string for missingness
    retval = []
    for x in df.to_dict('records'):
        parensplit = x['c_tooltip'].split('\\') # Compute this here bc hlevel is unreliable
        if len(parensplit)>2:
            txt = parensplit[2] + ":" + x['c_name']
        else:
            txt = x['c_name']
        retval.append(dbc.ListGroupItem(txt))
    return retval
    #return [dbc.ListGroupItem(x['c_tooltip'].split('\\')[2] + ":" + x['c_name']) if x['c_hlevel']>2 else  + ":" + x['c_name']) for x in df.to_dict('records')]

# Run this when the app starts to set the state of things
# Also updates the state JSON when a button is clicked or the dropdown is used
@app.callback(
    Output('app_state','children'),
    [Input({'type': 'navbutton', 'index': ALL}, 'n_clicks'),Input('zoom', 'n_clicks'), Input('unzoom', 'n_clicks'), Input('site', 'value'),Input('mainTabs','active_tab'),Input('slider_siteoutlier','value')],
    [State('items', 'value'), State('items', 'options'),State('app_state','children')]
)
def cbController(nclick_values,zoomclix,unzoomclix,site,tab,slider,checks,options,appstate):
    global conn,dbstyle, sites,globalDbFile
    if appstate=='app_state':
        # New version of Dash, cannot share sqlite across windows
        #initApp(db=globalDbFile)
        # Initialize the app
        c = conn.cursor()
        zoom_clix = 0
        unzoom_clix = 0
        c.execute("select min(c_hlevel) from bigfullname")
        hlevel = c.fetchone()[0]
        minhlevel = hlevel
        query = "select top 1 c_fullname from bigfullname where c_hlevel=?" if dbstyle=="MSSQL" else "select c_fullname from bigfullname where c_hlevel=? limit 1"
        c.execute(query, str(hlevel)) # Limit 1 for PGSQL
        pathstart = c.fetchone()[0]
        pathstart = pathstart[0:pathstart[1:].find('\\') + 1]
        path = [pathstart]
        site = 'All' if 'All' in sites else sites[0] # There must be at least 1 site
        app_state = {'action':'','zoom_clix': 0, 'unzoom_clix': 0, 'hlevel':hlevel,'minhlevel': minhlevel, 'path': path, 'site': site,'tab':tab, "slider":slider, 'selected':[], 'selected_new':""}
        return json.dumps(app_state)

    appstatedict = json.loads(appstate)

    # If slider was moved but not button click, or callback called on startup, or multiple checked or nothing checked
    unclix = 0 if unzoomclix is None else unzoomclix
    clix=0 if zoomclix is None else zoomclix

    if (slider and slider != appstatedict['slider']):
        # Tab changed
        appstatedict['slider']=slider
    if (tab and tab != appstatedict['tab']):
        # Tab changed
        appstatedict['tab']=tab
    if (site and site != appstatedict['site']):
        # Site changed!
        appstatedict['site'] = site if site else 'All'
        print("New site selected:" + site)
        print("Controller - New Site selected")
        appstatedict['action']='site'
    elif unclix != appstatedict['unzoom_clix']:
        appstatedict['unzoom_clix'] = unclix
        if appstatedict['hlevel'] > appstatedict['minhlevel']:
            appstatedict['hlevel'] = appstatedict['hlevel'] - 1
            appstatedict['path'] = appstatedict['path'][:-1]
            appstatedict['action']='unzoom'
            appstatedict['selected_new']=''
            appstatedict['selected']=[]
            print("Controller - Unzoom:" + str(appstatedict['path']))
    #elif len(checks) == 0 or len(checks) > 1:
    #    appstatedict['action']='none'
    #    print("Controller - no action")
    elif appstatedict['zoom_clix'] != clix:
        appstatedict['zoom_clix'] = clix
        appstatedict['hlevel'] = appstatedict['hlevel'] + 1
        # Use checkbox - appstatedict['path'].append(checks[0][checks[0][:-1].rfind('\\') + 1:-1])
        appstatedict['path'].append(appstatedict['selected_new'][appstatedict['selected_new'][:-1].rfind('\\') + 1:-1])
        appstatedict['action']='zoom'
        appstatedict['selected_new'] = ''
        appstatedict['selected'] = []
        print("Controller - Zoom:" + str(appstatedict['path']))

    # Nav buttons were clicked
    # Index is the index element of the id
    # nclicks is an int of the number of clicks for this button
    # values is a list of nclicks for all buttons in the current list
    if 'index' in dash.callback_context.triggered[0]['prop_id']:
        parsedContext = json.loads(dash.callback_context.triggered[0]['prop_id'][:-9])
        index=parsedContext['index']
        nclicks = dash.callback_context.triggered[0]['value']
        appstatedict['action']='navclick'
        appstatedict['selected_new']=index
        if index not in appstatedict['selected']:
            appstatedict['selected'].append(index)
        else:
            appstatedict['selected'].remove(index)

    return json.dumps(appstatedict)



# This is the callback when someone clicks the zoom button, which moves down the hierarchy
# It also needs to handle the base case of just setting the state of the items.
# THIS VERSION DOES IT WITH BUTTONS!
@app.callback(
    Output('navbuttons', 'children'),
#    [Input('zoom', 'n_clicks'), Input('unzoom', 'n_clicks')],
    [Input('app_state','children')],
    [State('items', 'value'), State('navbuttons', 'children')]
)
def cbNavigateButtons(state, checks, options):
    global conn
    if (state=='app_state'): return options
    appstatedict = json.loads(state)

    # Update only if we navigated the ontology or if we're in the outlier tab (which has a bunch of thinks)
    if appstatedict['action'] in ('zoom','unzoom','') or appstatedict['tab']=='siteoutlier_tab':
        #sql_select = "select distinct c_fullname AS value,c_name AS label from totalnums_oldcols t inner join bigfullname b on t.fullname_int=b.fullname_int "
        if appstatedict['tab']=='explorer_tab':
            sql_select = "select distinct c_visualattributes, c_fullname AS value,c_name AS label from totalnums_recent_joined "
        elif appstatedict['tab']=='siteoutlier_tab':
            sql_select = "select distinct  abs(pct-average)>(%s*stdev) as outlier,c_fullname AS value,c_name AS label, c_visualattributes from outliers_sites_pct t inner join bigfullname b on t.fullname_int=b.fullname_int " % (
                str(appstatedict['slider']))
        elif appstatedict['tab']=='missing_tab':
            sql_select = """select c_fullname, c_tooltip, c_hlevel, c_name from bigfullname fn inner join totalnums_recent r on r.fullname_int=fn.fullname_int 
              where fn.fullname_int not in (select fullname_int from totalnums_recent where site='{{}}') and c_visualattributes not like 'L%' and c_visualattributes not like '_H%'
              order by c_hlevel """
            sql_select = """select case when notin is null then 1 else 0 end outlier, c_fullname as value, c_name as label, c_visualattributes from 
             (select distinct fn.c_fullname, fn.c_tooltip, fn.c_hlevel, fn.c_name, fn.c_visualattributes, notin.fullname_int notin from bigfullname fn inner join totalnums_recent r on r.fullname_int=fn.fullname_int
              left outer join (select * from totalnums_recent where site='{{}}') notin on notin.fullname_int=fn.fullname_int
               where c_visualattributes not like 'L%' and c_visualattributes not like '_H%'
              order by c_hlevel) x """
            sql_select = sql_select.replace("{{}}", appstatedict['site'])
        else:
            sql_select = "select distinct c_fullname AS value,c_name AS label, c_visualattributes from totalnums_recent_joined "

        # Compute the items for the checkboxes and return
        # Special logic to get all items in ontology if missing tab or all sites are selected
        if appstatedict['site']=='All' or appstatedict['tab']=='missing_tab':
            sql_where=" where c_hlevel='%s' and c_fullname like '%s'" % (
                str(appstatedict['hlevel']), '\\'.join(appstatedict['path']) + '\\%')
        else:
            sql_where = " where c_hlevel='%s' and site='%s' and c_fullname like '%s'" % (
             str(appstatedict['hlevel']), appstatedict['site'], '\\'.join(appstatedict['path']) + '\\%')

        items = pd.read_sql_query(sql_select+sql_where, conn).to_dict('records')
        print(sql_select+sql_where)

        out = []
        for i in items:
            out.append(dbc.Button(i['label'],className="mr-1", id={'type':'navbutton','index':i['value']},
                                  style={'font-size':'10pt'},
                                  color=('danger' if 'outlier' in i and i['outlier']==1 else 'dark'),
                                  outline=(True if i['c_visualattributes'] in ('FAE','FA','FA ','CA ','CAE','CA') else False)))
        return out
    if appstatedict['action']=='navclick':
        appstatedict['selected']

    return options

# This callback draws the graph whenever checkboxes change or site is changed
@app.callback(
    Output('hlevel_graph', 'figure'),
    [Input('app_state', 'children')],
    [State('navbuttons', 'children'), State('hlevel_graph', 'figure')]
)
def cbLineGraphButtons(state, navbuttons,oldfig):
    global conn
    if (state=='app_state'): return {}
    start = time.time()
    appstatedict = json.loads(state)

    if appstatedict['action'] in ('navclick','zoom','site'):
        # Get just the available data in the df
        sql = "select distinct c_fullname,refresh_date,c_name,c from totalnums_oldcols t inner join bigfullname b on t.fullname_int=b.fullname_int where c_hlevel='%s' and site='%s' and c_fullname like '%s' order by refresh_date asc" % (
         appstatedict['hlevel'], appstatedict['site'], '\\'.join(appstatedict['path']) + '\\%')
        print(sql)
        dfsub = pd.read_sql_query(sql, conn)

        traces = []
        ymax = 0
        for n in appstatedict['selected']:
            xf = dfsub[dfsub.c_fullname == n]
            if len(xf) > 0:
                traces.append(
                    go.Scatter(x=xf['refresh_date'], y=xf['c'], text=xf.iloc[0, :].c_name, name=xf.iloc[0, :].c_name,
                               marker={'size': 15}, mode='lines+markers'))
                ymax=max(ymax,xf.groupby(by='c_fullname').max()['c'].values[0]) # Fix 11-19 - put the legend in the right place
                Cstd=xf['c'].std()
                Cmean=xf['c'].mean()
                Clow = Cmean - 3*Cstd
                if Clow<0: Clow=0
                #traces.append(go.Scatter(x=[xf['refresh_date'].min(),xf['refresh_date'].max()],y=[Cmean,Cmean],name='mean of '+xf.iloc[0,:].c_name,mode='lines')) # Mean
                #traces.append(go.Scatter(x=[xf['refresh_date'].min(), xf['refresh_date'].max()], y=[Cmean+3*Cstd, Cmean+3*Cstd],
                #                        name='high control of ' + xf.iloc[0, :].c_name, mode='lines'))
                #traces.append(go.Scatter(x=[xf['refresh_date'].min(), xf['refresh_date'].max()], y=[Clow, Clow],
                #                         name='low control of ' + xf.iloc[0, :].c_name, mode='lines'))
        print("Graph time:"+str(time.time()-start))
        layout =  {'legend':{'x':0,'y':ymax},'showlegend':True}
        return {'data': traces, 'layout': layout}
    return oldfig if oldfig is not None else {}

# This callback draws the bar graph whenever checkboxes change
@app.callback(
    Output('bars_graph', 'figure'),
    [Input('app_state', 'children')],
    [State('navbuttons', 'children'), State('hlevel_graph', 'figure')]
)
def cbBarGraphButtonsButtons(state,navbuttons,oldfig):
    global conn
    if (state=='app_state'): return {}
    start = time.time()
    appstatedict = json.loads(state)

    # Get just the available data in the df
    sql = "select distinct c_fullname,site,c_name,max(c) c from totalnums_oldcols t inner join bigfullname b on t.fullname_int=b.fullname_int where site!='All' and c_hlevel='%s' and c_fullname like '%s' group by c_fullname,site,c_name" % (
     appstatedict['hlevel'], '\\'.join(appstatedict['path']) + '\\%')
    dfsub = pd.read_sql_query(sql, conn)

    traces = []
    for n in appstatedict['selected']:
        xf = dfsub[dfsub.c_fullname == n]
        if len(xf) > 0:
            traces.append(
                go.Bar(x=xf['site'], y=xf['c'], text=xf.iloc[0, :].c_name, name=xf.iloc[0, :].c_name))
                          # marker={'size': 15}, mode='lines+markers'))
    print("Bar time:"+str(time.time()-start))
    return {'data': traces}

# This callback draws the bar graph whenever checkboxes change
@app.callback(
    Output('siteoutlier_graph', 'figure'),
    #[Input('items_siteoutlier', 'value')],
    [Input('app_state', 'children')],
    [State('navbuttons', 'children'), State('hlevel_graph', 'figure')]
)
def cbSiteoutlierGraph(state,navbuttons,oldfig):
    global conn
    if (state=='app_state'): return {}
    start = time.time()
    appstatedict = json.loads(state)
    if appstatedict['site'] and appstatedict['site']=='All': return {} # Not support All sites, must choose one for compare

    # Get just the available data in the df
    sql = "select distinct c_fullname,site,c_name,max(pct) c from totalnums_recent_pct t inner join bigfullname b on t.fullname_int=b.fullname_int where site!='All' and c_hlevel='%s' and c_fullname like '%s' group by c_fullname,site,c_name" % (
     appstatedict['hlevel'], '\\'.join(appstatedict['path']) + '\\%')
    dfsub = pd.read_sql_query(sql, conn)

    # Also get average
    sql = "select distinct c_fullname,c_name,average avg, (%s*stdev) stdev from outliers_sites_pct t inner join bigfullname b on t.fullname_int=b.fullname_int where site!='All' and c_hlevel='%s' and c_fullname like '%s' group by c_fullname,site,c_name" % (
     str(appstatedict['slider']),appstatedict['hlevel'], '\\'.join(appstatedict['path']) + '\\%')
    dfavg = pd.read_sql_query(sql, conn)

    traces = []
    n=appstatedict['selected_new']
    xf = dfsub[dfsub.c_fullname == n]
    xavg = dfavg[dfavg.c_fullname == n]
    graph_title= 'Nothing selected'

    # Add avg
    if len(xavg) > 0:
        traces.append(
            go.Bar(x=['average' for x in xavg['avg']], y=xavg['avg'], error_y=dict(type='data',visible=True,array=xavg['stdev']),
                   text=xavg.iloc[0, :].c_name, name='avg'))
        #traces.append(
        #    go.Bar(x=['avg + %s * stdev' % str(appstatedict['slider']) for x in xavg['stdev']], y=xavg['stdev'],
        #           text=xavg.iloc[0, :].c_name, name='%s * stdev' % str(appstatedict['slider'])))

    if len(xf) > 0:
        xf_site = xf[xf['site']==appstatedict['site']]
        xf_notsite=xf[xf['site']!=appstatedict['site']]
        # Site color red or green depending on outlier status
        print(xf_site['c'].iloc[0])
        site_color= 'rgba(204,50,50,1)' if (abs(xavg['avg']-xf_site['c'].iloc[0])-xavg['stdev']).iloc[0]>0 else 'rgba(50,204,50,1)'
        graph_title = xf_site.iloc[0, :].c_name
        # Site value
        traces.append(
            go.Bar(x=xf_site['site'], y=xf_site['c'],
                   marker={'color':site_color},text=xf_site.iloc[0, :].c_name, name=xf_site['site'].iloc[0]))
        # Other site values
        traces.append(
            go.Bar(x=xf_notsite['site'], y=xf_notsite['c'], text=xf_notsite.iloc[0, :].c_name,
                   marker={'color':'rgb(204,204,204,1)'},name='Other sites'))
        # Add selected site avg
        #traces.append(
        #     go.Bar(x=[appstatedict['site']], y=xavg['stdev'], text=xf.iloc[0, :].c_name, name=xf.iloc[0, :].c_name))

    print("Bar time:"+str(time.time()-start))
    layout = go.Layout(barmode='stack', title=graph_title)
    return {'data': traces,'layout':layout}

if __name__=='__main__':

    # MSSQL
    """    password_mssql = keyring.get_password(service_name='db.totalnums_mssql',username='i2b2')  # You need to previously have set it with set_password
        db = {'server':'localMSSQL','user':'i2b2','password':password_mssql,'db':'dbo'}
        db="DSN=localMSSQL;UID=i2b2;PWD="+password_mssql
        initApp(dbtype='MSSQL',db=db)
    """

    # SQLite
    globalDbFile = "/Users/jeffklann/HMS/Projects/ACT/totalnum_data/reports/totalnums.db"
    initApp(
        db="/Users/jeffklann/HMS/Projects/ACT/totalnum_data/reports/totalnums.db")

    app.run_server(debug=True,threaded=False)