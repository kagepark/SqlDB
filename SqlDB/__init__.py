#Kage Park
import kmisc as km
from kmisc.Import import *
#SQLite3
#import sqlite3
#Postgresql
#import psycopg2
#import psycopg2.extras

def SqlLike(field,find_src,mode='OR',sensitive=False):
    if not isinstance(find_src,(tuple,list)):
        find=['{}'.format(find_src)]
    else:
        find=list(find_src[:])
    for fnd in range(0,len(find)):
        if sensitive:
            if find[fnd][0] == '\n':
                find[fnd]="""(instr({0},'{1}') = 1 OR instr({0},'\n{1}') > 0)""".format(field,find[fnd][1:])
            elif find[fnd][-1] == '\n':
                find[fnd]="""(substr({0},-{2},{2}) = '{1}' OR instr({0},'{1}\n') > 0)""".format(field,find[fnd][:-1],len(find[fnd][:-1]))
            else:
                find[fnd]="""instr({0},'{1}') > 0""".format(field,find[fnd])
        else:
            find[fnd]="""{0} LIKE '{1}'""".format(field,find[fnd])
    if len(find) > 1:
        return '('+' {} '.format(mode).join(find)+')'
    else:
        return find[0]


def SqlLikeFormat(field,find_src,sensitive=False,mode='AND'):
    #[ A , B] : A and B
    # A = (a,b,c) : a or b or c
    if not isinstance(find_src,list):
        find=['{}'.format(find_src)]
    else:
        find=find_src[:]
    for i in range(0,len(find)):
        if isinstance(find[i],tuple):
            find[i]=SqlLikeFormat(field,list(find[i]),mode='OR',sensitive=sensitive)
        else:
            find[i]=find[i].replace('*','%')
            if find[i][0] == '^':
                if sensitive:
                    if find[i][-1] == '%':
                        find[i]=SqlLike(field,('\n'+find[i][1:-1],),mode='OR',sensitive=sensitive)
                    else:
                        find[i]=SqlLike(field,('\n'+find[i][1:]+' ',),mode='OR',sensitive=sensitive)
                else:
                    find[i]=SqlLike(field,(find[i][1:],'\n'+find[i][1:]),mode='OR',sensitive=sensitive)
            elif find[i][-1] == '$':
                if sensitive:
                    if find[i][0] == '%':
                        find[i]=SqlLike(field,(find[i][1:-1]+'\n',),mode='OR',sensitive=sensitive)
                    else:
                        find[i]=SqlLike(field,(' '+find[i][:-1]+'\n',),mode='OR',sensitive=sensitive)
                else:
                    find[i]=SqlLike(field,(find[i][:-1],find[i][:-1]+'\n'),mode='OR',sensitive=sensitive)
            else:
                if sensitive:
                    if find[i][0] == '%' and find[i][-1] == '%':
                        find[i]= """ instr({0},'{1}') > 0""".format(field,find[i][1:-1])
                    elif find[i][0] == '%':
                        find[i]= """ instr({0},'{1} ') > 0""".format(field,find[i][1:])
                    elif find[i][-1] == '%':
                        find[i]= """ instr({0},' {1}') > 0""".format(field,find[i][:-1])
                    else:
                        find[i]= """ instr({0},' {1} ') > 0""".format(field,find[i])
                else:
                    find[i]=""" {0} LIKE '{1}'""".format(field,find[i])
    if len(find) > 1:
        return '('+' {} '.format(mode).join(find)+')'
    else:
        return find[0]

def SqlMkData(values,decode=None):
    if decode:
        return tuple([ km._u_bytes2str(x,decode) if isinstance(x,str) else x for x in values])
    else:
        return tuple(values)


def SqlConInfo(**info):
    if info.get('conn'):
        conn=info.get('conn')
    else:
        if info['module'] == 'sqlite3':
            Import('sqlite3')
            db_file=info.get('db_file')
            if db_file:
                conn=sqlite3.connect(db_file)
                conn.row_factory=sqlite3.Row
        elif info['module'] in ['psql','postgresql']:
            Import('psycopg2')
            Import('psycopg2.extras')
            info['module']='psql'
            timeout=info.get('timeout',1)
            port=info.get('port',5432)
            user=info.get('user')
            passwd=info.get('passwd')
            ips=info.get('ip')
            db=info.get('db')
            if isinstance(ips,str): ips=ips.split(',')
            for ip in ips: 
                try:
                    conn = psycopg2.connect(database = db, user = user, password = passwd, host = ip, port = port,connect_timeout=timeout)
                    break
                except psycopg2.OperationalError as e:
                    print('Unable to connect! : {0}'.format(e))
    if info['module'] == 'psql':
        if info.get('row',dict) is dict:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = conn.cursor()
    else:
        if info.get('cur'):
            cur=info.get('cur')
        else:
            cur=conn.cursor()
    return {'conn':conn,'cur':cur,'info':info,'module':info['module']}


def SqlExec(sql,data=[],row=list,mode='fetchall',encode=None,**db):
    put_idx=None
    if sql is False: return False,data
    if db['module'] in ['psql','postgresql']:
        con_info=SqlConInfo(row=row,**db)
    else:
        con_info=SqlConInfo(**db)

    try:
        if data and isinstance(data,(tuple,list)):
            if isinstance(data,tuple):
                #convert data
                if mode.lower() in ['put','save','commit','update']:
                    data=tuple([km._u_bytes2str(x) if isinstance(x,(str,bytes)) else x for x in data])
                con_info['cur'].execute(sql,data)
            else:
                for row in data:
                    #convert data
                    if mode.lower() in ['put','save','commit','update']:
                        row=tuple([km._u_bytes2str(x) if isinstance(x,str) else x for x in row])
                    con_info['cur'].execute(sql,row)
        else:
            con_info['cur'].execute(sql)
    except (sqlite3.Error,) as e:
        return False,e
    except (Exception, psycopg2.Error) as e:
        return False,e

    if con_info['module'] == 'sqlite3':
        if row is dict:
            con_info['cur'].row_factory = lambda c,r:dict([(col[0], r[idx]) for idx, col in enumerate(con_info['cur'].description)])
        else:
            con_info['cur'].row_factory=None
    rt=[]
    if mode.lower() in ['put','save','commit','update']:
        con_info['cur'].execute('select last_insert_rowid();')
        idx=con_info['cur'].fetchone()
        con_info['conn'].commit()
        if idx:
            rt=idx[0]
        else:
            rt=True
    elif mode.lower() in ['one','single','get_one','get_single','fetchone']:
        rt=[i for i in con_info['cur'].fetchone()]
    else:
        rt=[i for i in con_info['cur'].fetchall()]
    con_info['conn'].close()
    return rt,None

def SqlAutoIdx(table_name,index='id'):
    cur,msg=SqlExec('''select max({}) from {};'''.format(index,table_name),row=list,**db_info)
    if cur is False:
        return False,msg
    if isinstance(cur,list) and cur:
        return True,cur[0][0]+1
    return True,1

def SqlTableInfo(with_field=False,**db):
    if db.get('module') in ['psql','postgresql']:
        cur,msg=SqlExec('''select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE';''',row=list,**db)
    else:
        cur,msg=SqlExec('''select name from sqlite_master where type='table';''',row=list,**db)
    if cur:
        if with_field:
            rt={}
            for tt in cur:
                if tt:
                    rt[tt[0]]=SqlFieldInfo(tt[0],field_mode='simple',**db)
            return rt
        else:
            return [tt[0] for tt in cur if tt]
    return msg

def SqlFieldInfo(table_name,field_mode='name',out=dict,**db):
    rt={}
    if not isinstance(table_name,str): return rt
    if db.get('module') in ['psql','postgresql']:
        #data_type similar but simple word is udt_name
        cur,msg=SqlExec('''SELECT ordinal_position,column_name,udt_name,is_nullable,column_default,character_maximum_length FROM information_schema.columns WHERE table_catalog='{}' and table_name = '{}';'''.format(db_info.get('db'),table_name),row=list,**db_info)
        if 'primary' in [out,field_mode]:
            return cur[0][1]
        if field_mode == 'simple': return [ item[1] for item in cur ]
        if isinstance(cur,list):
            pk=True
            for item in cur:
                notnull=False
                if item[3] == 'NO': notnull=True
                pk=False
                if item[0] == 1: pk=True
                if field_mode in ['name','field','name_info','field_info']:
                    rt[item[1]]={'idx':item[0],'type':item[2],'notnull':notnull,'dflt_value':item[4],'primary':pk,'len':item[5]}
                else:
                    rt[item[0]]={'name':item[1],'type':item[2],'notnull':notnull,'dflt_value':item[4],'primary':pk,'len':item[5]}
    else:
        cur,msg=SqlExec('''pragma table_info('{}')'''.format(table_name),row=list,**db)
        #cid:name:type:notnull:dflt_value:pk
        #Int(Column ID):String(Column name):String(Column Type):bool(Has a not Null constraint):object(default Value):bool(Is part of the Primary Key)
        if 'primary' in [out,field_mode]:
            for ii in cur:
                if ii[5] == 1: return ii[1]
        if field_mode == 'simple': return [ item[1] for item in cur ]
        if isinstance(cur,list):
            for item in cur:
                _type_a=item[2].split('(')
                _type=_type_a[0]
                ln=0
                if len(_type_a) == 2:
                    ln=_type_a[1].split(')')[0]
                notnull=False
                if item[-1] == 0: notnull=True
                pk=False
                if item[5] == 1: pk=True
                if field_mode in ['name','field','name_info','field_info']:
                    rt[item[1]]={'idx':item[0],'type':_type,'len':ln,'notnull':notnull,'dflt_value':item[4],'primary':pk}
                else:
                    rt[item[0]]={'name':item[1],'type':_type,'len':ln,'notnull':notnull,'dflt_value':item[4],'primary':pk}
    if out in ['list',list]:
        if field_mode in ['name','field','name_info','field_info']:
            return [  {name:{'name':name,'type':rt[name]['type'],'len':rt[name]['len'],'notnull':rt[name]['notnull'],'dflt_value':rt[name]['dflt_value'],'primary':rt[name]['primary'],'idx':rt[name]['idx']}} for name in rt ]
        else:
            return [  {idx:{'name':rt[idx]['name'],'type':rt[idx]['type'],'len':rt[idx]['len'],'notnull':rt[idx]['notnull'],'dflt_value':rt[idx]['dflt_value'],'primary':rt[idx]['primary'],'idx':idx}} for idx in rt ]
    return rt


def SqlPut(tablename,rows,fields=[],decode=None,check=False,dbg=False,**db):
    if 'mode' in db: db.pop('mode')
    def MkSql(tablename,keys):
        fields=','.join(keys)
        question_marks=','.join('?'*len(keys))
        return 'INSERT INTO '+tablename+' ('+fields+') VALUES ('+question_marks+')'

    def single_dict_row(tablename,row,decode=None):
        #Ignore None value
        kk=[]
        vv=[]
        for ii in row:
            if row[ii] is not None:
                kk.append(ii)
                vv.append(row[ii])
        return MkSql(tablename,kk), SqlMkData(vv,decode)

    def single_list_row(tablename,row,keys=[],decode=None):
        if isinstance(keys,(list,tuple)) and len(keys) == len(row):
            return MkSql(tablename,keys), SqlMkData(row,decode)
        return None,None

    field_info=None
    if check: field_info=SqlFieldInfo(tablename,mode='name',out=dict,**db)

    if isinstance(rows,(list,tuple)):
        if rows and not isinstance(rows[0],(dict,tuple,list)):
            cc=SqlCheckFields(tablename,row_field=rows,field_info=field_info,**db)
            if cc[0] is False: return cc
            sql,values=single_list_row(tablename,rows,keys=fields,decode=decode)
            if sql and values:
                if dbg:
                    print('sql={}, values={}'.format(sql,values))
                    return True,'sql={}, values={}'.format(sql,values)
                else:
                    tmp,msg=SqlExec(sql,values,mode='commit',**db)
                    return [tmp],msg
            return False,values
        rows_idx=[]
        for row in rows:
            if isinstance(row,dict):
                cc=SqlCheckFields(tablename,row_dict=row,field_info=field_info,**db)
                if cc[0] is False: return cc
                sql,values=single_dict_row(tablename,row,decode=decode)
            elif isinstance(row,(tuple,list)):
                cc=SqlCheckFields(tablename,row_field=field_info,field_info=field_info,**db)
                if cc[0] is False: return cc
                sql,values=single_list_row(tablename,row,keys=fields,decode=decode)
            if dbg:
                print('sql={}, values={}'.format(sql,values))
            else:
                tmp,msg=SqlExec(sql,values,mode='commit',**db)
                rows_idx.append(tmp)
        return rows_idx,msg
    return False,None

def SqlUpdate(tablename,rows,fields=[],condition=[],decode=None,dbg=False,**db):
    '''
    rows=[<data>,...] or [{<field name>:<data>}] : Update data 
    fields=[<field name>,...] for rows with [<data>,...]
    condition=[{<field name>:<data>}] : Update data find condition
    '''
    if 'mode' in db: db.pop('mode')
    #UPDATE <Table> SET <Field> = <Val> WHERE <Field>='<find>'
    #cur,conn,db,info=SqlConInfo(cur)
    def MkSql(tablename,keys,condition=None,values=[]):
        # Need update ","
        keys=[ '{}=?'.format(i) for i in keys ]
        sql='UPDATE '+tablename+' SET '+','.join(keys)
        if condition:
            sql=sql+ ' WHERE'
            sql,tmp=SqlWhere(sql,[],condition)
            if tmp:
                for ii in tmp:
                    values.append(ii)
        return sql

    def single_dict_row(tablename,row,decode=None,condition=None):
        # ignore None value
        kk=[]
        vv=[]
        for ii in row:
            if row[ii] is not None:
                kk.append(ii)
                vv.append(row[ii])
        sql=MkSql(tablename,kk,condition,values=vv)
        return sql,SqlMkData(vv,decode)

    def single_list_row(tablename,row,keys=[],decode=None,condition=None):
        sql=MkSql(tablename,keys,condition=condition,values=row)
        return sql,SqlMkData(row,decode)

    if isinstance(rows,dict):
        sql,values=single_dict_row(tablename,rows,decode=decode,condition=condition)
        if sql and values:
            if dbg:
                print('sql={}, values={}'.format(sql,values))
                return True,None
            else:
                tmp,conn=SqlExec(sql,values,mode='commit',**db)
                return tmp,conn
        return False,'SQL and Value is not matched'
    elif isinstance(rows,(list,tuple)):
        if rows and not isinstance(rows[0],(dict,tuple,list)):
            sql,values=single_list_row(tablename,rows,keys=fields,decode=decode,condition=condition)
            if sql and values:
                if dbg:
                    print('sql={}, values={}'.format(sql,values))
                    return True,'sql={}, values={}'.format(sql,values)
                else:
                    tmp,conn=SqlExec(sql,values,mode='commit',**db)
                    return tmp,conn
            return False,values
        for row in rows:
            if isinstance(row,dict):
                sql,values=single_dict_row(tablename,row,decode=decode,condition=condition)
            elif isinstance(row,(tuple,list)):
                sql,values=single_list_row(tablename,row,keys=fields,decode=decode,condition=condition)
            if sql and values:
                if dbg:
                    print('sql={}, values={}'.format(sql,values))
                    tmp=True
                else:
                    tmp,conn=SqlExec(sql,values,mode='commit',**db)
        return tmp,conn
    return False,values


def SqlCheckFields(table_name,row_dict={},row_field=[],field_info=None,**db):
    if field_info is None: field_info=SqlFieldInfo(table_name,field_mode='name',out=dict,**db)
    if isinstance(field_info,dict):
        notnull=[ ii for ii in field_info if ii == 'notnull' and field_info[ii]]
        if row_dict: 
            chk_fields=row_dict.keys()
        elif row_field:
            chk_fields=row_field
        for ii in notnull:
            if ii not in chk_fields: return False,'Missing {}'.format(ii)
        # check Data Type
    return True,'OK'

def SqlFilterFields(table_name,check_field_names=[],**db):
    field_info=SqlFieldInfo(table_name,field_mode='simple',**db)
    return [ ii for ii in check_field_names if ii in field_info ]

def SqlGet(sql=None,tablename=None,find=[],out_fields=[],order=[],group=[],row=list,dbg=False,filterout=True,mode='all',**db):
    '''
    sql=<SQL Format String>, If None then make from info, rows, out_fiels
    info=[<table name>,<SQL Command>]
    find=[{<field name>:<data>}] for info or [<data>,...] for sql
    out_fields=[<fieldname>,...] : *: get all fields
    '''
    if 'mode' in db: db.pop('mode')
    # Filter out for wrong field name
    if filterout: out_fields=SqlFilterFields(tablename,out_fields,**db)

    values=[]
    if isinstance(sql,str):
        if '?' in sql and isinstance(find,(list,tuple)) and len([ i for i in sql if i == '?']) == len(find):
            sqlexec,msg=SqlExec(sql,tuple(find),row=row,mode=mode,**db)
            return sqlexec
    elif tablename:
        if out_fields:
            out_field=','.join(out_fields)
        else:
            out_field='*'
        sql='SELECT {} FROM {}'.format(out_field,tablename)
        if isinstance(find,(list,tuple)) and find:
            sql=sql+' WHERE'
            for r in find:
                sql,tmp=SqlWhere(sql,values,r)
        if isinstance(group,(list,tuple)) and group:
            sql=sql+' GROUP BY '+','.join(group)
        if isinstance(order,(list,tuple)) and order:
            sql=sql+' ORDER BY '+','.join(order)
    if dbg:
        print('sql={}, data={}'.format(sql,values))
        return 'sql={}, data={}'.format(sql,values)
    try:
        return SqlExec(sql,tuple(values),row=row,mode=mode,**db)
    except Exception as e:
        return False,'{}'.format(e)


def SqlDel(sql=None,tablename=None,find=[],dbg=False,**db):
    '''
    sql=<SQL Format String>, If None then make from info, rows, out_fiels
    find=[{<field name>:<data>}] for info or [<data>,...] for sql
    '''
    if 'mode' in db: db.pop('mode')
    values=[]
    if isinstance(sql,str):
        if '?' in sql and isinstance(find,(list,tuple)) and len([ i for i in sql if i == '?']) == len(find):
            values=find
    elif tablename:
        sql='DELETE FROM {}'.format(tablename)
        if isinstance(find,(list,tuple)):
            sql=sql+' WHERE'
            for r in find:
                sql,tmp=SqlWhere(sql,values,r)
    if dbg:
        print('sql={}, data={}'.format(sql,values))
        return 'sql={}, data={}'.format(sql,values)
    try:
        return SqlExec(sql,tuple(values),mode='commit',**db)
    except Exception as e:
        return False,'{}'.format(e)


def SqlWhere(sql,values,sub,field=None,mode=None):
    def dict_sql(sql,field,mods,symbol=False,mode=None):
        mod=next(iter(mods))
        modl=km.Lower(mod)
        if modl in ['and','or']:
            sql,m=SqlWhere(sql,values,mods,field=field)
        else:
            if symbol: sql=sql+' {}'.format(mode)
            if modl == 'like':
                sql=sql+' '+SqlLikeFormat(field,mods[mod],sensitive=False)
                return sql,None
            elif modl in ['sensitive','sens']:
                sql=sql+' '+SqlLikeFormat(field,mods[mod],sensitive=True)
                return sql,None
            else:
                if modl == 'is':
                    if mods[mod] is None: 
                        modv='null'
                    elif isinstance(mods[mod],str) and 'None' in mods[mod]:
                        modv=mods[mod].replace('None','null')
                    else:
                        modv=mods[mod]
                    sql=sql+' {} is {}'.format(field,modv)
                    return sql,None
                else:
                    sql=sql+' {} {} ?'.format(field,mod)
                    return sql,mods[mod]

    if isinstance(sub,dict):
        S_AND=sub.get('and')
        S_OR=sub.get('or')
        if S_AND:
            if isinstance(S_AND,dict):
                field=next(iter(S_AND))
                S_AND=S_AND[field]
            sql,m=SqlWhere(sql,values,S_AND,field=field,mode='AND')
        elif S_OR:
            if isinstance(S_OR,dict):
                field=next(iter(S_OR))
                S_OR=S_OR[field]
            sql,m=SqlWhere(sql,values,S_OR,field=field,mode='OR')
        else:
            field=next(iter(sub))
            if not sub[field]:
                sql=False
                values.append('No search data(%s) for "%s" field <= ex: {<field>:{<operator>:<find data>}}'%(sub[field],field))
            else:
                if isinstance(sub[field],dict):
                    #AND/OR : {<field>:{'or/and':({<oper>:<find data>},...)}}
                    if 'and' in sub[field]:
                        sql,m=SqlWhere(sql,values,sub[field],field=field,mode='AND')
                    elif isinstance(sub[field],dict) and 'or' in sub[field]:
                        sql,m=SqlWhere(sql,values,sub[field],field=field,mode='OR')
                    else:
                        #Single data: {<field>:{<oper>:<find data>}}
                        sql,m=dict_sql(sql,field,sub[field])
                        if m is not None: values.append(m)
                elif isinstance(sub[field],(list,tuple)): # AND/OR's tuple list (relate)
                    sql,m=dict_sql(sql,field,sub[field])
                    if m is not None: values.append(m)
                else: # Wrong format
                    sql=False
                    values.append('Wrong Format(%s) <= ex: {<field>:{<operator>:<find data>}}'%(sub))
    elif isinstance(sub,(list,tuple)):
        symbol=False
        sub_symbol=False
        sql=sql+' ('
        for mods in sub:
            if field is None:
                if sub_symbol : sql=sql+' {}'.format(mode)
                sql,m=SqlWhere(sql,values,mods,field=field)
                sub_symbol=True
            else:
                sql,m=dict_sql(sql,field,mods,symbol,mode)
                if m is not None: values.append(m)
                symbol=True
        sql=sql+' )'
    return sql,values

if __name__ == '__main__':
    ######################################################
    #conn=SqlConInfo(module='sqlite3',db_file='database.db')
    #create='''CREATE TABLE IF NOT EXISTS demo (
    #    idx INTEGER PRIMARY KEY AUTOINCREMENT,
    #    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    #    name TEXT NULL,
    #    rank int 0
    #);'''
    #cc,con=SqlExec(create,mode='commit',**db_info)
    db_info={
            'ip':['192.168.122.5','192.168.122.50'],
            'db':'db_name',
            'user':'user_id',
            'passwd':'PassWd',
            'port':5432,
            'timeout':10,
            'module':'psql',
            }
    table_name='demo'
    index='idx'
    cur,msg=SqlExec('''SELECT idx,created_at,rank,name FROM demo;''',row=dict,**db_info)
    print(cur)
    print(msg)
    print(SqlFieldInfo(table_name,field_mode='simple',out=dict,**db_info))

