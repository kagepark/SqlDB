#Kage Park
import sys
import time
import traceback
from kmport import *
#SQLite3
#import sqlite3
#Postgresql
#import psycopg2
#import psycopg2.extras

try:
    # yum install sqlcipher
    # pip3 install pysqlcipher3
    from pysqlcipher3 import dbapi2 as sqlcipher
    enc=True
except:
    enc=False

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


def SqlLikeFormat(field,find_src,sensitive=False,mode='AND',NOT=False):
    #[ A , B] : A and B
    # A = (a,b,c) : a or b or c
    if not isinstance(find_src,list):
        find=['{}'.format(find_src)]
    else:
        find=find_src[:]
    for i in range(0,len(find)):
        if isinstance(find[i],tuple):
            find[i]=SqlLikeFormat(field,list(find[i]),mode='OR',sensitive=sensitive,NOT=NOT)
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
                    if NOT:
                        find[i]=""" {0} NOT LIKE '{1}'""".format(field,find[i])
                    else:
                        find[i]=""" {0} LIKE '{1}'""".format(field,find[i])
    if len(find) > 1:
        return '('+' {} '.format(mode).join(find)+')'
    else:
        return find[0]

def SqlMkData(values,decode=None):
    if decode:
        return tuple([ Str(x,decode) if isinstance(x,str) else x for x in values])
    else:
        return tuple(values)

def SmartSqlMkData(table_name,key,data,decode=None,correct_count=False,**db_info):
    field_info=SqlFieldInfo(table_name,out=dict,**db_info)
    if isinstance(data,tuple): data=list(data)
    if correct_count and len(key) != len(data): return False,f'mismatched key({len(key)}) and data({len(data)}) count'
    for i in range(0,len(key)):
        if key[i] not in field_info: return False,f'Key({key[i]}) not found in Table({table_name})'
        data_type=field_info[key[i]].get('type').lower()
        if not isinstance(data[i],int) and not data[i]:
            if field_info[key[i]].get('notnull'):
                if data[i] is None:
                    data[i]='null'
                else:
                    continue
            else:
                return False,f'key({key[i]}) not null. but the data is None'
        if data_type in ['integer','int'] and not isinstance(data[i],int):
            data[i]=int(data[i])
        elif data_type in ['char','text']:
            if decode:
                data[i]=Str(data[i],decode)
            else:
                data[i]=Str(data[i])
        elif data_type in ['bool']:
            data[i]='true' if isinstance(data[i],(bool,int)) else 'false'
        elif data_type in ['blob'] and not isinstance(data[i],bytes):
            data[i]=Bytes(data[i])
    return tuple(data),'ok'

def SqlConInfo(**info):
    conn=None
    cur=None
    if info.get('conn'):
        conn=info.get('conn')
    else:
        module=info.get('module')
        if module == 'sqlite3':
            Import('sqlite3')
            db_file=info.get('db_file')
            if db_file:
                #Encripted DB
                if info.get('enc_key'):
                    if enc:
                        conn=sqlcipher.connect(db_file)
                        conn.execute('pragma key="{}"'.format(info.get('enc_key')))
                        conn.row_factory=sqlcipher.Row
                    else:
                        print('Please install sqlcipher(yum install sqlcipher) and pysqlcipher3(pip3 install pysqlcipher3)')
                else:
                    conn=sqlite3.connect(db_file)
                    conn.row_factory=sqlite3.Row
        elif module in ['psql','postgresql']:
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
    if conn:
        if module == 'psql':
            if info.get('row',dict) is dict:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cur = conn.cursor()
        elif module == 'sqlite3':
            if info.get('cur'):
                cur=info.get('cur')
            else:
                cur=conn.cursor()
    return {'conn':conn,'cur':cur,'info':info,'module':info['module']}


def SqlExec(sql,data=[],row=list,mode='fetchall',encode=None,raw=False,**db):
    return NewSqlExe(sql,value=data,row=row,table_name=None,**db)
    #put_idx=None

    #if sql is False: return False,data
    #if db.get('module') in ['psql','postgresql']:
    #    con_info=SqlConInfo(row=row,**db)
    #else:
    #    con_info=SqlConInfo(**db)

    #def __sql_exe__(sql,data,con_info,raw=False):
    #    if data:
    #        if sql.count('?') == 1:
    #            if not isinstance(data,(tuple,list)):
    #                data=(data,)
    #        if isinstance(data,(tuple,list)):
    #            #Single line data
    #            if isinstance(data,tuple) and sql.count('?') == len(data):
    #                #convert data
    #                if not raw:
    #                    if mode.lower() in ['put','save','commit','update']:
    #                        data=tuple([Str(x) if isinstance(x,(str,bytes)) else x for x in data])
    #                con_info['cur'].execute(sql,data)
    #            else:
    #                #multi line data
    #                for irow in data:
    #                    #convert data
    #                    if isinstance(irow,tuple) and sql.count('?') == len(irow):
    #                        if not raw:
    #                            if mode.lower() in ['put','save','commit','update']:
    #                                irow=tuple([Str(x) if isinstance(x,str) else x for x in irow])
    #                    con_info['cur'].execute(sql,irow)
    #    else:
    #        try:
    #            con_info['cur'].execute(sql)
    #        except:
    #            e=sys.exc_info()[0]
    #            er=traceback.format_exc()
    #            return False,'{}\n{}'.format(e,er)

    #if db.get('module') in ['psql','postgresql']:
    #    try:
    #        __sql_exe__(sql,data,con_info,raw)
    #    except (Exception, psycopg2.Error) as e:
    #        return False,e
    #else:
    #    try:
    #        nn=__sql_exe__(sql,data,con_info,raw)
    #        if isinstance(nn,tuple) and nn[0] is False:
    #            return nn
    #    except (sqlite3.Error,) as e:
    #        return False,e


#   #try:
#   #    if data and isinstance(data,(tuple,list)):
#   #        if isinstance(data,tuple):
#   #            #convert data
#   #            if mode.lower() in ['put','save','commit','update']:
#   #                data=tuple([Str(x) if isinstance(x,(str,bytes)) else x for x in data])
#   #            con_info['cur'].execute(sql,data)
#   #        else:
#   #            for row in data:
#   #                #convert data
#   #                if mode.lower() in ['put','save','commit','update']:
#   #                    row=tuple([Str(x) if isinstance(x,str) else x for x in row])
#   #                con_info['cur'].execute(sql,row)
#   #    else:
#   #        con_info['cur'].execute(sql)
#   #except (Exception, sqlite3.Error,) as e:
#   #    return False,e
#   #except (Exception, psycopg2.Error) as e:
#   #    return False,e

    #if con_info['module'] == 'sqlite3':
    #    if row is dict:
    #        con_info['cur'].row_factory = lambda c,r:dict([(col[0], r[idx]) for idx, col in enumerate(con_info['cur'].description)])
    #    else:
    #        con_info['cur'].row_factory=None
    #rt=[]
    #if mode.lower() in ['put','save','commit','update']:
    #    con_info['cur'].execute('select last_insert_rowid();')
    #    idx=con_info['cur'].fetchone()
    #    try:
    #        con_info['conn'].commit()
    #    except sqlite3.OperationalError as e:
    #        ok=0
    #        for i in range(0,10):
    #            #print('>> retry({}/5) DB commit after 1 second for {}'.format(i,e))
    #            time.sleep(1)
    #            try:
    #                con_info['conn'].commit()
    #                ok=1
    #                break
    #            except sqlite3.OperationalError as e:
    #                pass
    #        if ok == 0:
    #            con_info['conn'].close()
    #            return False,e
    #    if idx:
    #        rt=idx[0]
    #    else:
    #        rt=True
    #elif mode.lower() in ['one','single','get_one','get_single','fetchone']:
    #    rt=[i for i in con_info['cur'].fetchone()]
    #else:
    #    rt=[i for i in con_info['cur'].fetchall()]
    #con_info['conn'].close()
    #return rt,None

def SqlAutoIdx(table_name,index='id',**db):
    #data,msg=SqlExec('''select max({}) from {};'''.format(index,table_name),row=list,**db)
    data,msg=NewSqlExe('''select max({}) from {};'''.format(index,table_name),row=list,**db)
    if data is False:
        return False,msg
    if isinstance(data,list) and data:
        return True,data[0][0]+1
    return True,1

def SqlTableInfo(with_field=False,**db):
    if db.get('module') in ['psql','postgresql']:
        #data,msg=SqlExec('''select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE';''',row=list,**db)
        data,msg=NewSqlExe('''select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE';''',row=list,**db)
    else:
        #data,msg=SqlExec('''select name from sqlite_master where type='table';''',row=list,**db)
        data,msg=NewSqlExe('''select name from sqlite_master where type='table';''',row=list,**db)
    if data:
        if with_field:
            rt={}
            for tt in data:
                if tt:
                    rt[tt[0]]=SqlFieldInfo(tt[0],field_mode='simple',**db)
            return rt
        else:
            return [tt[0] for tt in data if tt]
    return msg

def SqlFieldInfo(table_name,field_mode='name',out=dict,**db):
    rt={}
    if not isinstance(table_name,str): return rt
    if db.get('module') in ['psql','postgresql']:
        #data_type similar but simple word is udt_name
        sql='''SELECT ordinal_position,column_name,udt_name,is_nullable,column_default,character_maximum_length FROM information_schema.columns WHERE table_catalog='{}' and table_name = '{}';'''.format(db.get('db'),table_name)
        #data,msg=SqlExec(sql,row=list,**db)
        data,msg=NewSqlExe(sql,row=list,**db)
        if isinstance(data,bool): return data,msg
        if 'primary' in [out,field_mode]:
            return data[0][1]
        if field_mode == 'simple': return [ item[1] for item in data ]
        if isinstance(data,list):
            pk=True
            for item in data:
                notnull=False
                if item[3] == 'NO': notnull=True
                pk=False
                if item[0] == 1: pk=True
                if field_mode in ['name','field','name_info','field_info']:
                    rt[item[1]]={'idx':item[0],'type':item[2],'notnull':notnull,'dflt_value':item[4],'primary':pk,'len':item[5]}
                else:
                    rt[item[0]]={'name':item[1],'type':item[2],'notnull':notnull,'dflt_value':item[4],'primary':pk,'len':item[5]}
    else:
        #data,msg=SqlExec('''pragma table_info('{}')'''.format(table_name),row=list,**db)
        data,msg=NewSqlExe('''pragma table_info('{}')'''.format(table_name),row=list,**db)
        #cid:name:type:notnull:dflt_value:pk
        #Int(Column ID):String(Column name):String(Column Type):bool(Has a not Null constraint):object(default Value):bool(Is part of the Primary Key)
        if isinstance(data,bool): return data,msg
        if 'primary' in [out,field_mode]:
            for ii in data:
                if ii[5] == 1: return ii[1]
        if field_mode == 'simple': return [ item[1] for item in data ]
        if isinstance(data,list):
            for item in data:
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
        #return MkSql(tablename,kk), SqlMkData(vv,decode)
        return MkSql(tablename,kk), SmartSqlMkData(tablename,kk,vv,decode,**db)[0]

    def single_list_row(tablename,row,keys=[],decode=None):
        if isinstance(keys,(list,tuple)) and len(keys) == len(row):
            #return MkSql(tablename,keys), SqlMkData(row,decode)
            return MkSql(tablename,keys), SmartSqlMkData(tablename,keys,row,decode,**db)[0]
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
                    #tmp,msg=SqlExec(sql,values,mode='commit',**db)
                    tmp,msg=NewSqlExe(sql,value=values,**db)
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
                #tmp,msg=SqlExec(sql,values,mode='commit',**db)
                tmp,msg=NewSqlExe(sql,value=values,**db)
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
        #return sql,SqlMkData(vv,decode)
        return sql, SmartSqlMkData(tablename,kk,vv,decode,**db)[0]

    def single_list_row(tablename,row,keys=[],decode=None,condition=None):
        sql=MkSql(tablename,keys,condition=condition,values=row)
        #return sql,SqlMkData(row,decode)
        return sql, SmartSqlMkData(tablename,keys,row,decode,**db)[0]

    if isinstance(rows,dict):
        sql,values=single_dict_row(tablename,rows,decode=decode,condition=condition)
        if sql and values:
            if dbg:
                print('sql={}, values={}'.format(sql,values))
                return True,None
            else:
                #tmp,conn=SqlExec(sql,values,mode='commit',**db)
                tmp,conn=NewSqlExe(sql,value=values,**db)
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
                    #tmp,conn=SqlExec(sql,values,mode='commit',**db)
                    tmp,conn=NewSqlExe(sql,value=values,**db)
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
                    #tmp,conn=SqlExec(sql,values,mode='commit',**db)
                    tmp,conn=NewSqlExe(sql,value=values,**db)
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
    if isinstance(field_info,tuple): return field_info
    return [ ii for ii in check_field_names if ii in field_info ]

def SqlGet(sql=None,tablename=None,find=[],out_fields=[],order=[],group=[],row=list,dbg=False,filterout=True,mode='all',**db):
    return NewSql(sql=sql,tablename=tablename,find=find,out_fields=out_fields,order=order,group=group,row=row,filterout=filterout,mode=mode,dbg=dbg,**db)
#def SqlGet(sql=None,tablename=None,find=[],out_fields=[],order=[],group=[],row=list,dbg=False,filterout=True,mode='all',**db):
#    '''
#    sql=<SQL Format String>, If None then make from info, rows, out_fiels
#    info=[<table name>,<SQL Command>]
#    find=[{<field name>:<data>}] for info or [<data>,...] for sql
#    out_fields=[<fieldname>,...] : *: get all fields
#    '''
#    if 'mode' in db: db.pop('mode')
#    # Filter out for wrong field name
#    if filterout:
#         out_fields=SqlFilterFields(tablename,out_fields,**db)
#         if isinstance(out_fields,tuple): return out_fields
# 
#    values=[]
#    if isinstance(sql,str):
#        #if '?' in sql and isinstance(find,(list,tuple)) and len([ i for i in sql if i == '?']) == len(find):
#        if find and isinstance(find,(list,tuple)):
#            if '?' in sql and len([ i for i in sql if i == '?']) == len(find):
#                data,msg=SqlExec(sql,tuple(find),row=row,mode=mode,**db)
#        else:
#            data,msg=SqlExec(sql,row=row,mode=mode,**db)
#        if isinstance(data,bool): return data,msg
#        return data,msg
#    elif tablename:
#        if out_fields:
#            out_field=','.join(out_fields)
#        else:
#            out_field='*'
#        sql='SELECT {} FROM {}'.format(out_field,tablename)
#        if isinstance(find,(list,tuple)) and find:
#            sql=sql+' WHERE'
#            for r in find:
#                sql,tmp=SqlWhere(sql,values,r)
#        if isinstance(group,(list,tuple)) and group:
#            sql=sql+' GROUP BY '+','.join(group)
#        if isinstance(order,(list,tuple)) and order:
#            sql=sql+' ORDER BY '+','.join(order)
#    if dbg:
#        print('sql={}, data={}'.format(sql,values))
#        return 'sql={}, data={}'.format(sql,values)
#    try:
#        return SqlExec(sql,tuple(values),row=row,mode=mode,**db)
#    except Exception as e:
#        return False,'{}'.format(e)


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
        #return SqlExec(sql,tuple(values),mode='commit',**db)
        return NewSqlExe(sql,value=tuple(values),**db)
    except Exception as e:
        return False,'{}'.format(e)


def SqlWhere(sql,values,sub,field=None,mode=None):
    def dict_sql(sql,field,mods,symbol=False,mode=None):
        mod=next(iter(mods))
        modl=Str(mod).lower() if isinstance(mod,(str,bytes)) else mod
        if modl in ['and','or']:
            sql,m=SqlWhere(sql,values,mods,field=field)
        else:
            if symbol: sql=sql+' {}'.format(mode)
            if modl == 'like':
                sql=sql+' '+SqlLikeFormat(field,mods[mod],sensitive=False)
                return sql,None
            elif modl == 'notlike':
                sql=sql+' '+SqlLikeFormat(field,mods[mod],sensitive=False,NOT=True)
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

def FTS_init(table_name,fields,key='id',version=3,**db):
    istable=IsTable('{}_fts'.format(table_name),**db)
    if not istable:
        if isinstance(fields,str): fields=fields.split(',')
        if key in fields: fields.remove(key)
        if isinstance(fields,list): fields=','.join(fields)

        #create
        #data,msg=SqlExec('''create virtual table {0}_fts using fts{3}({2},{1},content='{0}');'''.format(table_name,fields,key,version),mode='commit',**db)
        data,msg=NewSqlExe('''create virtual table {0}_fts using fts{3}({2},{1},content='{0}');'''.format(table_name,fields,key,version),**db)
        if data is False:
            return data,msg
        #initialize(copy data)
        #data,msg=SqlExec('''insert into {0}_fts ({2},{1}) select {2},{1} from {0};'''.format(table_name,fields,key),mode='commit',**db)
        data,msg=NewSqlExe('''insert into {0}_fts ({2},{1}) select {2},{1} from {0};'''.format(table_name,fields,key),**db)
        #automation
        new_fields=[]
        for i in fields.split(','):
            new_fields.append('new.{}'.format(i))
        sql='''CREATE TRIGGER {0}_fts_insert AFTER INSERT ON {0}
BEGIN
INSERT INTO {0}_fts({3},{1}) values (new.{3},{2});
END; '''.format(table_name,fields,','.join(new_fields),key)
        if IsSame(version,3):
            update_fields=[]
            for i in fields.split(','):
                if i == key: continue
                update_fields.append('{0}=new.{0}'.format(i))
            sql=sql+'''
CREATE TRIGGER {0}_fts_delete AFTER DELETE ON {0}
BEGIN
DELETE FROM {0}_fts WHERE {1} = old.{1};
END;
CREATE TRIGGER {0}_fts_update AFTER UPDATE ON {0}
BEGIN
UPDATE {0}_fts SET {2} WHERE {1} = old.{1};
END;
'''.format(table_name,key,','.join(update_fields))
        elif IsSame(version,5):
            old_fields=[]
            for i in fields.split(','):
                old_fields.append('old.{}'.format(i))
            sql=sql+'''
CREATE TRIGGER {0}_fts_delete AFTER DELETE ON {0}
BEGIN
INSERT INTO {0}_fts({4},{1}) values ('delete',old.{4},{3});
END;
CREATE TRIGGER {0}_fts_update AFTER UPDATE ON {0}
BEGIN
INSERT INTO {0}_fts({4},{1}) values ('delete',old.{4},{3});
INSERT INTO {0}_fts({4},{1}) values (new.{4},{2});
END;
'''.format(table_name,fields,','.join(new_fields),','.join(old_fields),key)
        #data,msg=SqlExec(sql,mode='commit',multi=True,**db)
        data,msg=NewSqlExe(sql,**db)
        return data,msg

def FTS(table_name,search=None,out_fields=None,group_field=None,search_field=None,key='id',row=dict,order=None,version=3,**db):
    group_field_and_rule=False
    def make_group_field(group_field,avail_fields):
        group_field_a=group_field.split(':')
        if group_field_a[0] in avail_fields:
            find_strings=':'.join(group_field_a[1:])
            try:
                find_int=int(find_strings)
            except:
                find_int=False
            if "'" in find_strings:
                search_sql="""( {}="{}" ) """.format(group_field_a[0],find_strings)
            else:
                if find_int:
                    search_sql="""( {0}='{1}' or {0}={1} ) """.format(group_field_a[0],find_strings)
                else:
                    search_sql="""({}='{}') """.format(group_field_a[0],find_strings)
            return True,search_sql
        else:
            return False,group_field_a[0]

    search_sql=''
    if out_fields is None:
        out_fields='*'
    elif isinstance(out_fields,list):
        out_fields=','.join(out_fields)
    if not search_field: search_field='{}_fts'.format(table_name)
    if group_field:
        avail_fields=SqlFieldInfo('{}_fts'.format(table_name),field_mode='name',out=dict,**db)
        if isinstance(group_field,list):
            gfield=[]
            for ss in group_field:
                ok,ss_sql=make_group_field(ss,avail_fields)
                if ok:
                    gfield.append(ss_sql)
            if gfield:
                if group_field_and_rule:
                    search_sql=search_sql+' and '.join(gfield)
                else:
                    search_sql=search_sql+' or '.join(gfield)
            else:
                return False,'Not found group filed: {}'.format(search_sql)
        else:
            ok,search_sql=make_group_field(group_field,avail_fields)
            if not ok:
                return False,'Not found group filed: {}'.format(search_sql)
    if search:
        if search_sql: search_sql=search_sql+' and '
        if "'" in search:
            search_sql=search_sql+""" {} match "{}" """.format(search_field,search)
        else:
            search_sql=search_sql+""" {} match '{}' """.format(search_field,search)
    if search_sql: search_sql=''' where {} '''.format(search_sql)
    # select id,subject,title from memo_fts where subject='python' and memo_fts match 'flaskco*';
    if order:
        #data,msg=SqlExec('''select {2} from {0} where {3} in (select {3} from {0}_fts {1}) order by {4} ;'''.format(table_name,search_sql,out_fields,key,order),row=row,**db)
        data,msg=NewSqlExe('''select {2} from {0} where {3} in (select {3} from {0}_fts {1}) order by {4} ;'''.format(table_name,search_sql,out_fields,key,order),row=row,**db)
    else:
        #data,msg=SqlExec('''select {2} from {0} where {3} in (select {3} from {0}_fts {1});'''.format(table_name,search_sql,out_fields,key,search_field),row=row,**db)
        data,msg=NewSqlExe('''select {2} from {0} where {3} in (select {3} from {0}_fts {1});'''.format(table_name,search_sql,out_fields,key,search_field),row=row,**db)
    return data,msg

def IsTable(table_name,**db):
    #data,msg=SqlExec('''select name from sqlite_master where type='table' and name='{}';'''.format(table_name),row=dict,**db)
    data,msg=NewSqlExe('''select name from sqlite_master where type='table' and name='{}';'''.format(table_name),row=dict,**db)
    if data:
        return True
    return False

def GetTablenames(**db):
    #data,msg=SqlExec('''select name from sqlite_master where type='table' and name!='sqlite_sequence';''',row=dict,**db)
    data,msg=NewSqlExe('''select name from sqlite_master where type='table' and name!='sqlite_sequence';''',row=dict,**db)
    return data,msg


#############################################################################
# New design (2024)
#############################################################################
def MkTableInDB(sql=None,**info):
    module=info.get('module')
    if module in ['psql','postgres']:
        #code here
        pass
    else:
        src_db=info.get('db_file')
        if not isinstance(src_db,str):
            return False,'Wrong DB file'
        base_dir=os.path.dirname(src_db)
        if base_dir and not os.path.isdir(base_dir):
            return False,'Not found database directory:{}'.format(base_dir)
        Import('sqlite3')
        if not sql:
            table_name=info.get('table_name')
            fields=info.get('fields')
            sql=f'''CREATE TABLE IF NOT EXISTS {table_name} ('''
            if isinstance(fields,(list,tuple)):
                sql=sql+', '.join(fields)
            sql=sql+');'
        o=NewSqlExe(sql,**info)
        if isinstance(o,tuple) and not o[0]:
            return o
        return True,'created sql'
    return False,'unknown module'

def GetTableSeq(src_db,table_name=None,create=False):
    #Get sequence number of tables in db file
    if not create and not os.path.isfile(src_db):
        return False,'Source DB file({}) not found'.format(src_db),'Source DB file({}) not found'.format(src_db)
    # If not found Extend file then create Extend DB file for writing new Data
    Import('sqlite3')
    src_conn=sqlite3.connect(src_db)   # existing DB file
    src_conn.row_factory=sqlite3.Row
    scur=src_conn.cursor()
    # Update ID information to expend
    scur.execute("select * from sqlite_sequence")
    seq_table=scur.fetchall()
    tables=[]
    tables_name=[]
    for t in seq_table:
        if table_name and table_name != t['name']: continue
        tables.append(t)
        tables_name.append(t['name'])
    return src_conn,tables,tables_name

def AddTableSeq(dest_db,tables,tables_name,idx=None):
    #copy source(tables) to dest_db
    # idx: None: copy last number, int: copy information but change sequence number to idx
    dest_conn,dest_tables,dest_tables_name=GetTableSeq(dest_db,create=True)
    for i in tables_name:
        if i in dest_tables_name:
            return False,f'Alreay {i} in sqlite_sequence of {dest_db}'

    dcur=dest_conn.cursor()
    for i,t in enumerate(tables):
        ik=t.keys()
        query='INSERT INTO sqlite_sequence (%(key)s) VALUES (%(val)s)' % {
            'key':','.join(ik),
            'val':','.join(('?',) * len(ik))
        }
        if isinstance(idx,int):
            v=[idx if c == 'seq' else tables_name[i] if c=='name' else t[c] for c in ik]
        else:
            v=[tables_name[i] if c=='name' else t[c] for c in ik]
        dcur.execute(query, v)
    dest_conn.commit()
    dest_conn.close()

def UpdateTableSeq(src_db,idx,src_table=None):
    src_conn,tables,tables_name=GetTableSeq(src_db,table_name=src_table)
    if src_conn is False: return src_conn,tables
    scur=src_conn.cursor()
    if not isinstance(idx,int):
        return False,'idx must be int'

    query='UPDATE sqlite_sequence SET '
    if src_table:
        if src_table not in tables_name:
            return False,'source table({}) not found'.format(src_table)
        t=tables[tables_name.index(src_table)]
        ik=t.keys()
        #Update
        q=''
        v=[]
        for i in ik:
            if i == 'seq':
                q=q+', seq={}'.format(idx) if q else 'seq={}'.format(idx)
            else:
                q=q+', {}=?'.format(i) if q else '{}=?'.format(i)
                v.append(t[i])
        scur.execute(query+q, v)
    else:
        for t in tables:
            ik=t.keys()
            q=''
            v=[]
            for i in ik:
                if i == 'seq':
                    q=q+', seq={}'.format(idx) if q else 'seq={}'.format(idx)
                else:
                    q=q+', {}=?'.format(i) if q else '{}=?'.format(i)
                    v.append(t[i])
            scur.execute(query+q, v)
    src_conn.commit()
    src_conn.close()

def CloneDBTable(src_db,source_table=None,dest_table=None,dest_db=None,seq=0,copy_data=False):
    if not os.path.isfile(src_db):
        return False,'Source DB file({}) not found'.format(src_db)
    if not dest_db:
        if not dest_table:
            return False,'Dest Table not input'
        if not source_table:
            return False,'If Same DB file then clone Table to Other name Table, but not input source table name'

    # If not found Extend file then create Extend DB file for writing new Data
    Import('sqlite3')
    src_conn=sqlite3.connect(src_db)   # existing DB file
    src_conn.row_factory=sqlite3.Row
    scur=src_conn.cursor()
    scur.execute('SELECT * from sqlite_master')
    data=scur.fetchall()
    tables=[]
    tables_name=[]
    for t in filter(lambda r:r['type'] == 'table', data):
        if t['type'] == 'table' and t['name'] != 'sqlite_sequence':
            tables.append(t)
            tables_name.append(t['name'])
    if dest_db:
        src_conn.close() # No more use source DB

        dest_conn=sqlite3.connect(dest_db)   # existing DB file
        dest_conn.row_factory=sqlite3.Row
        dcur=dest_conn.cursor()
        dcur.execute('SELECT * from sqlite_master')
        dest_data=dcur.fetchall()
        dtables_name=[]
        for t in filter(lambda r:r['type'] == 'table', dest_data):
            if t['type'] == 'table' and t['name'] != 'sqlite_sequence':
                dtables_name.append(t['name'])
        if not source_table and not dest_table: # clone file to file for Table schema
            for t in tables:
                sn=t['name']
                if sn in dtables_name:
                    return False,f'{sn} in dest db({dest_db})'
                dest_sql=t['sql']
                dcur.execute(dest_sql)
            dest_conn.commit()
            dest_conn.close()

            #Update SEQ
            s_conn,seq_source,seq_source_name=GetTableSeq(src_db)
            s_conn.close()
            AddTableSeq(dest_db,seq_source,seq_source_name,idx=seq) # copy source seq to dest

        elif isinstance(source_table,str) and source_table: # Copy Source to Dest DB File
            source_table_a=source_table.split(',')
            if len(source_table_a) == 1 and isinstance(dest_table,str) and dest_table:# Copy source to changed name dest table in dest DB File
                if dest_table in dtables_name:
                    return False,f'{dest_name} is in dest DB({dest_db})'
                if source_table not in tables_name:
                    return False,f'{source_table} not in source DB({src_db})'
                dest_sql=tables[tables_name.index(source_table)]['sql']
                dest_sql=dest_sql.replace(f'TABLE {source_table} (',f'TABLE {dest_table} (')
                dcur.execute(dest_sql)
                dest_conn.commit()
                dest_conn.close()
                #Update SEQ
                s_conn,seq_source,seq_source_name=GetTableSeq(src_db,table_name=source_table)
                s_conn.close()
                AddTableSeq(dest_db,seq_source,[dest_table],idx=seq) # copy source seq to dest

            else: # Copy Source tables to Dest DB File
                seq_d=[]
                seq_dn=[]
                for s in source_table_a:
                    if s in dtables_name:
                        print(f'{s} in dest db({dest_db})')
                        continue
                    if s in tables_name:
                        dest_sql=tables[tables_name.index(s)]['sql']
                        dcur.execute(dest_sql)
                        s_conn,seq_source,seq_source_name=GetTableSeq(src_db,table_name=s)
                        s_conn.close()
                        seq_d.append(seq_source[0])
                        seq_dn.append(seq_source_name[0])
                dest_conn.commit()
                dest_conn.close()

                #Update SEQ
                AddTableSeq(dest_db,seq_d,seq_dn,idx=seq) # copy source seq to dest

    else: # Copy Local Table to changed Table name in self db file
        if not source_table or not dest_table:
            return False,'It required source table name and dest table name'
        if source_table not in tables_name:
            return False,f'source({source_table}) not found in current DB'
        if dest_table in tables_name:
            return False,f'dest({dest_table}) found in current DB'
        dest_sql=tables[tables_name.index(source_table)]['sql']
        dest_sql=dest_sql.replace(f'TABLE {source_table} (',f'TABLE {dest_table} (')
        scur.execute(dest_sql)
        src_conn.commit()
        src_conn.close()

        #Update SEQ
        s_conn,seq_source,seq_source_name=GetTableSeq(src_db,table_name=source_table)
        s_conn.close()
        AddTableSeq(src_db,seq_source,[dest_table],idx=seq) # copy source seq to dest

    if copy_data: # copy data
        CloneDBTableData(src_db,dest_db,source_table,dest_table,clone=True)
    return True,'ok'

def CloneDBTableData(src_db,dest_db,source_table=None,dest_table=None,stack=False,clone=True,ignore_fields=[]):
    #src_db: source DB FIle
    #source_table: None: Copy all table in the src_db, <name>: copy only the <name>
    #dest_db: dest DB FIle
    #dest_table: None: same as source_table, <name>: change the table name
    #  if dest_table exist then source_table also single
    #stack : copy data to stack at existing data
    #clone : copy all data to same even primary key too
    #ignore_fields: it required clone=False, copy data wihout ignore_fields

    if stack: clone=False
    if not os.path.isfile(src_db):
        return False,'Source DB file({}) not found'.format(src_db)
    if not dest_db: dest_db=src_db
    if not os.path.isfile(dest_db):
        return False,'Dest DB file({}) not found'.format(dest_db)

    def _clone_(cur,conn,stable_name,dtable_name):
        cur.execute(f'SELECT * from {stable_name}')
        rows=cur.fetchall()
        if rows:
           placeholders=', '.join(['?' for _ in range(len(rows[0]))])
           conn.executemany(f"INSERT INTO {dtable_name} VALUES ({placeholders});", rows)

    def _sel_copy_(stable_name,src_db,cur,dtable_name,conn,stack,ignore_fields):
        if not isinstance(ignore_fields,list): ignore_fields=[]
        fields=[]
        fields_info=SqlFieldInfo(stable_name,field_mode='name',db_file=src_db)
        for fnn in fields_info:
            if stack and fields_info[fnn]['primary']: continue
            if fnn in ignore_fields: continue
            fields.append(fnn)
        cur.execute(f"SELECT {','.join(fields)} from {stable_name}")
        rows=scur.fetchall()
        query='INSERT INTO %(dest_table)s (%(key)s) VALUES (%(val)s)' % {
          'dest_table':dtable_name,
          'key':','.join(fields),
          'val':','.join(('?',) * len(fields))
        }
        dcur=conn.cursor()
        for rr in rows:
           dcur.execute(query, rr)

    # If not found Extend file then create Extend DB file for writing new Data
    Import('sqlite3')
    src_table_name=[a['name'] for a in GetTablenames(db_file=src_db)[0]]
    dest_table_name=[a['name'] for a in GetTablenames(db_file=dest_db)[0]]
    if not source_table and not dest_table: # clone file to file for Table schema
        if src_db == dest_db:
            return False,'Same DB, can not copy to self'
        dest_conn=sqlite3.connect(dest_db)
        src_conn=sqlite3.connect(src_db)   # existing DB file
        scur=src_conn.cursor()
        for stn in src_table_name:
            if stn not in dest_table_name:
                return False,f"{stn} not in {dest_db}"
            if stn not in dest_table_name:
                return False,f'{stn} not in dest DB({dest_db})'
            if clone:
                _clone_(scur,dest_conn,stn,stn)
            else:
                _sel_copy_(stn,src_db,scur,stn,dest_conn,stack,ignore_fields)
        src_conn.close()
        dest_conn.commit()
        dest_conn.close()

    elif isinstance(source_table,str) and source_table: # Copy Source to Dest DB File
        source_table_a=source_table.split(',')
        if len(source_table_a) == 1 and isinstance(dest_table,str) and dest_table:# Copy source to changed name dest table in dest DB File
            if dest_table not in dest_table_name:
                return False,f'{dest_name} not in dest DB({dest_db})'
            if source_table not in src_table_name:
                return False,f'{source_table} not in source DB({src_db})'
            dest_conn=sqlite3.connect(dest_db)
            src_conn=sqlite3.connect(src_db)   # existing DB file
            scur=src_conn.cursor()
            if clone:
                _clone_(scur,dest_conn,source_table,dest_table)
            else:
                _sel_copy_(source_table,src_db,scur,dest_table,dest_conn,stack,ignore_fields)
            src_conn.close()
            dest_conn.commit()
            dest_conn.close()
        else: # Copy Source tables to Dest DB File
            dest_conn=sqlite3.connect(dest_db)
            src_conn=sqlite3.connect(src_db)   # existing DB file
            scur=src_conn.cursor()
            for stn in source_table_a:
               if stn not in dest_table_name:
                   return False,f'{stn} not in dest DB({dest_db})'
               if clone:#Clone
                   _clone_(scur,dest_conn,stn,stn)
               else:
                   _sel_copy_(stn,src_db,scur,stn,dest_conn,stack,ignore_fields)
            src_conn.close()
            dest_conn.commit()
            dest_conn.close()
    else: # Copy Local Table to changed Table name in self db file
        if dest_table not in dest_table_name:
            return False,f'{dest_name} not in dest DB({dest_db})'
        if source_table not in src_table_name:
            return False,f'{source_table} not in source DB({src_db})'
        if source_table == dest_table:
            return False,f'can not copy it self'
        src_conn=sqlite3.connect(src_db)   # existing DB file
        scur=src_conn.cursor()
        if clone: #Copy to backend
            _clone_(scur,src_conn,source_table,dest_table)
        else:
            _sel_copy_(source_table,src_db,scur,dest_table,src_conn,stack,ignore_fields)
        src_conn.commit()
        src_conn.close()

def TableIDRangeInDBFile(table_name,conn=None,db_file=None):
    close_conn=False
    if not conn:
        #if not conn then open db_file
        Import('sqlite3')
        if not isinstance(db_file,str) or not os.path.isfile(db_file):
            return False,f'DB File({db_file}) not found'
        conn=sqlite3.connect(db_file)   # existing DB file
        close_conn=True
    dcur=conn.cursor()
    dcur.execute(f"SELECT id FROM {table_name} ORDER BY id ASC LIMIT 1")
    start=dcur.fetchone()
    dcur.execute(f"select seq from sqlite_sequence where name='{table_name}'")
    end=dcur.fetchone()
    if close_conn:
        # if opened db_file then close, if given connection then not close
        conn.close()
    if not start:
        if not end:
            return 0,0
        return end[0],end[0]
    else:
        return start[0],end[0]

def Conn(db_file,enc_key=None):
    #Import('sqlite3')
    if not isinstance(db_file,str) or not os.path.isfile(db_file):
        return False

    #Encripted DB
    if enc_key:
        if enc:
            conn=sqlcipher.connect(db_file)
            conn.execute(f'pragma key="{enc_key}"')
            conn.row_factory=sqlcipher.Row
        else:
            print('Please install sqlcipher(yum install sqlcipher) and pysqlcipher3(pip3 install pysqlcipher3)')
            return False
    else:
        conn=sqlite3.connect(db_file)
        conn.row_factory=sqlite3.Row
    return conn
#    return sqlite3.connect(db_file)   # existing DB file

def GetCursor(con,row_format=list,module='sqlite3',cmd=None):
    #ROW Format convert if need dict
    #return Cursor()
    if module in ['psql','postgras']:
        if row_format in [dict,'dict']:
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = con.cursor()
    else:
        cur=con.cursor()

        if cmd in ['delete','update','insert']: return cur 
        if row_format in [dict,'dict']:
            cur.row_factory = lambda c,r:dict([(col[0], r[idx]) for idx, col in enumerate(cur.description)])
        else:
            cur.row_factory=None
    return cur

def NewSqlExe(sql,db_file=None,value=None,row=list,int_primary_in_table=False,table_name=None,**db):
    Import('sqlite3')
    if not isinstance(sql,str): return False, f'SQL({sql}) required String'
    sql_cmd=sql.lower().split()[0]
    sql_qc=sql.count('?')
    out=[]
    if not db_file: db_file=db.get('db_file')
    if isinstance(db_file,str): db_file=db_file.split(',')
    elif isinstance(db_file,tuple): db_file=list(db_file)
    if not isinstance(db_file,list):
        return False,f'missing db_file parameter'
    elif not os.path.isfile(db_file[0]):
        return False,f'First DB File({db_file[0]}) not found'
    single=False
    if sql_cmd == 'insert' and len(db_file) > 1: # insert data at new db file
        if os.path.isfile(db_file[0]) and not os.path.isfile(db_file[-1]):
            ok,msg=CloneDBTable(db_file[0],dest_db=db_file[-1],seq=None,copy_data=False)
            if not ok: return ok,msg
        db_file=[db_file[-1]]
    mx_db=len(db_file)
    con=None
    for i in range(0,mx_db):
        if not isinstance(db_file[i],str) or not os.path.isfile(db_file[i]): continue
        if isinstance(int_primary_in_table,int) and table_name:
            single=True
            con=Conn(db_file[i],enc_key=db.get('enc_key'))
            sr,er=TableIDRangeInDBFile(table_name,con)
            if isinstance(sr,bool) or int_primary_in_table < sr or er < int_primary_in_table:
                con.close()
                continue
        else:
            con=Conn(db_file[i],enc_key=db.get('enc_key'))
        dcur=GetCursor(con,row)
        try:
            if value and '?' in sql:
                if isinstance(value,(tuple,list)):
                    if sql_qc == len(value):
                        dcur.execute(sql,tuple(value))
                    else:
                        return False,f'mismatched between data({len(value)}) and parameters({sql_qc}) number1'
                        #return False,f'mismatched between data({value}) and parameters({sql}) number1'
                elif sql_qc == 1:
                    dcur.execute(sql,(value,))
                else:
                    return False,f'mismatched between data({len(value)}) and parameters({sql_qc}) number2'
                    #return False,f'mismatched between data({value}) and parameters({sql}) number2'
            else:
                dcur.execute(sql)
        except:
            con.close()
            e=sys.exc_info()[0]
            er=traceback.format_exc()
            print('ERR:{}\n{}'.format(e,er))
            return False,'{}\n{}'.format(e,er)
        if sql_cmd in ['delete','update','insert','create']:
            con.commit()
        else:
            if single:
                o=dcur.fetchone()
                out.append(o)
            else:
                o=dcur.fetchall()
                for i in o:
                    out.append(i)
        con.close()
    if con:
        return out,'ok'
    else:
        return False,f'All DB File not found({db_file})'

def mk_list(a,_type=(list,)):
    if isinstance(a,str): 
        return a.split(',')
    elif isinstance(a,tuple):
        return list(a)
    elif not a:
        return []
    elif not isinstance(a,list):
        return [a]
    return a

def NewSql(sql=None,tablename=None,find=[],out_fields=[],order=[],group=[],row=list,filterout=True,mode='all',idx=None,value=[],dbg=False,**db):
    if 'mode' in db: db.pop('mode')
    # Filter out for wrong field name
    value=mk_list(value)
    condition_value=[]
    if not sql and tablename:
        if filterout:
            db_file=db.get('db_file')
            if isinstance(db_file,list): db_file=db_file[0]
            out_fields=SqlFilterFields(tablename,out_fields,db_file=db_file)
            if isinstance(out_fields,tuple): return out_fields
        out_fields=mk_list(out_fields)
        order=mk_list(order)
        group=mk_list(group)
        find=mk_list(find)
        if out_fields:
            out_field=','.join(out_fields)
        else:
            out_field='*'
        sql='SELECT {} FROM {}'.format(out_field,tablename)
        if isinstance(find,(list,tuple)) and find:
            sql=sql+' WHERE'
            for r in find:
                sql,tmp=SqlWhere(sql,condition_value,r)
        if isinstance(group,(list,tuple)) and group:
            sql=sql+' GROUP BY '+','.join(group)
        if isinstance(order,(list,tuple)) and order:
            sql=sql+' ORDER BY '+','.join(order)

    if dbg: print(f'>> SQL: {sql}, value:{value+condition_value}')
    db_file=db.get('db_file')
    out=[]
    con=None
    #Need code update to using SmartSqlMkData()
    #if tablename and keys:
    #    datas=SmartSqlMkData(tablename,keys,row,decode,**db)
    #else:
    datas=value+condition_value
    if isinstance(idx,int): #primary key condition
        return NewSqlExe(sql,db_file,datas,row,int_primary_in_table=idx,table_name=tablename)
    else: #other condition
        out,msg=NewSqlExe(sql,db_file,datas,row)
        if isinstance(out,bool): return out,msg
    return out,'ok'

if __name__ == '__main__':
    ######################################################
    db_info={'module':'sqlite3','db_file':'database.db'}
    create='''CREATE TABLE IF NOT EXISTS demo (
        idx INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        SS Char(5) Null,
        name TEXT NULL,
        rank int default 0,
        ok bool default false,
        vector blob
    );'''
    cc,con=SqlExec(create,mode='commit',**db_info)
    #db_info={
    #        'ip':['192.168.122.5','192.168.122.50'],
    #        'db':'db_name',
    #        'user':'user_id',
    #        'passwd':'PassWd',
    #        'port':5432,
    #        'timeout':10,
    #        'module':'psql',
    #        }
    table_name='demo'
    index='idx'
    cur,msg=SqlExec('''SELECT idx,created_at,rank,name FROM demo;''',row=dict,**db_info)
    print(cur)
    print(msg)
    #print(SqlFieldInfo(table_name,field_mode='simple',out=dict,**db_info))
    print(SqlFieldInfo(table_name,out=dict,**db_info))

