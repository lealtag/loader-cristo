import  pprint,math,json,signal, pyodbc, local, decimal, calendar, datetime, config, urllib.request, urllib.error, sys, logging, logging.handlers, time

username = "loaderfroyo01@lealtag.com"
password = "1234"

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        return super(DecimalEncoder, self).default(o)




class RESTHandler(logging.Handler):
    """
    A handler class which sends log strings to a wx object
    """
    def __init__(self,host,port,url):
        """
        Initialize the handler
        @param wxDest: the destination object to post the event to 
        @type wxDest: wx.Window
        """
        logging.Handler.__init__(self)
        self.host = host
        self.port = port
        self.url = url

    def flush(self):
        """
        does nothing for this handler
        """


    def emit(self, record):
        """
        Emit a record.


        """
        try:
            data = {'level': record.levelname, 'message': record.getMessage(), "date":record.asctime, "local":local.local['name']}
            jsonS = json.dumps(data, cls=DecimalEncoder)
            headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
            headersD = {'X-COIN':'h4kun4m4t4t4k4p15c4bul','ID':local.local['id']}
            headers.update(headersD)
            post_data = jsonS.encode('utf-8')
            #print(jsonS)
            request = urllib.request.Request(self.host+':'+self.port+self.url, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        except Exception as e:
            #print(e)
            f = open(config.params["log_error_file"]+'.dead', 'a')
            f.write(time.strftime('%d/%m/%Y %H:%M:%S')+' # CONNECTION ERROR # '+self.host+':'+self.port+self.url+'\n')
            f.close()



def update_progress(progress,qt,tot):
    logging.debug("Progreso - "+str(qt)+"/"+str(tot)+" elementos enviados ("+str(progress)+"%)")


def recover(code):
    if code > 0 and code != 422:
        return False
    return True

def get_products(cursor,id_local,auth):
    try:
        q0_select = ['code','description','price']
        if config.params["init"]:
            cursor.execute("SELECT CodProd AS code, Descrip AS description, precio1 AS price FROM saprod WHERE codprod IN (SELECT DISTINCT codProd FROM saeprd WHERE MtoVentas > 0 ) AND fechaUV < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")
            
        else:
            cursor.execute("SELECT CodProd AS code, Descrip AS description, precio1 AS price FROM saprod WHERE codprod IN (SELECT DISTINCT codProd FROM saeprd WHERE MtoVentas > 0 ) AND fechaUV >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")
        
        rows = cursor.fetchall()
        #print(rows)
        #print(json.dump(rows))
        if len(rows) > 0:

            n = len(rows)
            qt = 0
            pivote = 1
            porcentaje = (100 // 20) * pivote
            progreso = math.ceil(n / 20) * pivote

            logging.info("%d PRODUCT RECORDS WILL BE SENT DURING THIS LOAD PROCESS",n)

            fail = []

            for row in rows:
                product = {}
        
                for i in range(len(q0_select)):
                    
                    product[q0_select[i]] = getattr(row,q0_select[i])
                
                product['_id']=id_local+'_'+product['code']
                product['local']=id_local
        
                package = json.dumps(product, cls=DecimalEncoder)                
                response = sender(config.params["url"],config.params["port"],config.params["products"],package,auth)
                
                if(response == 600):

                    ri = 0
                    while(ri < 2 and response == 600):

                        response = sender(config.params["url"],config.params["port"],config.params["products"],package,auth)
                        ri += 1

                if not recover(response):

                    fail.append(product['_id'])

                qt += 1
                if qt == progreso:
                    update_progress(porcentaje, qt,n)
                    pivote += 1
                    porcentaje = (100 // 20) * pivote
                    progreso = math.ceil(n / 20) * pivote
                
            return fail

            
        else:

            logging.info("NO PRODUCT RECORDS WILL BE SENT DURING THIS LOAD PROCESS ")
            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET PRODUCTS, EXCEPTION : [%s]",e)
        return None     
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

def get_clients(cursor,id_local,no_id,auth):  
    try:

        if config.params["init"]:
            cursor.execute(" SELECT DISTINCT a.id3 as client from safact a where a.tipoFac='A' and a.signo=1 and a.fechaT < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' ")
        else:
            cursor.execute(" SELECT DISTINCT a.id3 as client from safact a where a.tipoFac='A' and a.signo=1 and a.fechaT >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' ")

        invoices = cursor.fetchall()

        if len(invoices) < 1:

            logging.info("NO CLIENT RECORDS WILL BE SENT DURING THIS LOAD PROCESS")
            return []


        a = []
        for row in invoices:

            a.append(getattr(row,'client'))

        query = '(' + ', '.join("'{0}'".format(w) for w in a) + ')'

        
        q0_select = ['_id','name','address']
       
        cursor.execute("SELECT codClie as _id, Descrip as name, direc1 as address from saclie where codClie in " + query)    
        
        rows = cursor.fetchall()
        if len(rows) > 0 :

            n = len(rows)
            qt = 0
            pivote = 1
            porcentaje = (100 // 20) * pivote
            progreso = math.ceil(n / 20) * pivote

            logging.info("%d CLIENT RECORDS WILL BE SENT DURING THIS LOAD PROCESS",n)

            fail = []
            for row in rows:
                client = {}
        
                for i in range(len(q0_select)):
                    client[q0_select[i]] = getattr(row,q0_select[i])
                
                for name in no_id:
                    
                    if client['_id']==name:
                        client['_id']=id_local+'_NOID'
                
        
                package = json.dumps(client, cls=DecimalEncoder)                
                response = sender(config.params["url"],config.params["port"],config.params["clients"],package,auth)

                if(response == 600):

                    ri = 0
                    while(ri < 2 and response == 600):

                        response = sender(config.params["url"],config.params["port"],config.params["clients"],package,auth)
                        ri += 1

                if not recover(response):
                    fail.append(client['_id'])

                qt += 1
                if qt == progreso:
                    update_progress(porcentaje, qt,n)
                    pivote += 1
                    porcentaje = (100 // 20) * pivote
                    progreso = math.ceil(n / 20) * pivote

            
            return fail
            
            
        else:
            logging.info("NO CLIENT RECORDS WILL BE SENT DURING THIS LOAD PROCESS")

            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET CLIENTS, EXCEPTION : [%s]",e)
        return None
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

def get_invoices(cursor,id_local,auth):
    
    
    try:
        q0_select = ['number','date','client','subtotal','tax','total', 'product', 'quantity']
      
        # SE AGREGO EL PEO DEL DESCUENTO, PERO NO SE ESTA DISCRIMINANDO ESTE TIPO DE FACTURAS SOBRE LAS DEMAS
        # SE AGREGO LA CANTIDAD DE SAINT PARA DISCRIMINAR UNITARIAS  
        if config.params["init"]:
            cursor.execute(" SELECT a.Numerod as number, a.fechaT as date, a.id3 as client, (a.monto-a.descto1) as subtotal, a.mtoTax as tax, a.contado as total, b.codItem as product, count(b.codItem) as quantity, sum(b.cantidad) as qtPerItem, sum(b.totalItem) as tot from safact a , saitemfac b where a.tipoFac='A' and a.signo=1 and a.numeroD=b.numeroD and a.fechaT < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' group by a.Numerod, a.fechaT, a.id3, a.monto, a.mtoTax, a.contado, a.descto1, b.codItem order by a.Numerod")
        else:
            cursor.execute(" SELECT a.Numerod as number, a.fechaT as date, a.id3 as client, (a.monto-a.descto1) as subtotal, a.mtoTax as tax, a.contado as total, b.codItem as product, count(b.codItem) as quantity, sum(b.cantidad) as qtPerItem, sum(b.totalItem) as tot from safact a , saitemfac b where a.tipoFac='A' and a.signo=1 and a.numeroD=b.numeroD and a.fechaT >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' group by a.Numerod, a.fechaT, a.id3, a.monto, a.mtoTax, a.contado, a.descto1, b.codItem order by a.Numerod")
        
        rows_0 = cursor.fetchall()
        if len(rows_0) > 0 :


            if config.params["init"]:
                cursor.execute("SELECT count(*) as ct  from safact a  where a.tipoFac='A' and a.signo=1 and a.fechaT < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")
            else:
                cursor.execute("SELECT count(*) as ct from safact a  where a.tipoFac='A' and a.signo=1 and a.fechaT >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")
            raux = cursor.fetchall()
            n = int(getattr(raux[0],'ct'))
            logging.info("%d INVOICE RECORDS WILL BE SENT DURING THIS LOAD PROCESS",n)
            
            qt = 0
            pivote = 1
            porcentaje = (100 // 20) * pivote
            progreso = math.ceil(n / 20) * pivote
            

            invoices = []
        
            row=rows_0[0]
            invoice = {}
                
            invoice['number']= getattr(row,'number')
            invoice['date']= calendar.timegm(getattr(row,'date').utctimetuple())
            invoice['client']= getattr(row,'client')
            if invoice['client'] in config.params['no_id'] :
                        invoice['client']= id_local+'_NOID'
            invoice['subtotal']= getattr(row,'subtotal')
            invoice['tax']= getattr(row,'tax')
            invoice['total']= getattr(row,'total')
            invoice['local']=id_local
        
        
            product_id = getattr(row,'product')
            # IF CHIMBO PARA LOS HELADOS
            #print(product_id)
            if id_local == "Froyo01" and product_id == '01':
                invoice['products']= [ {'pr':id_local+"_"+product_id,'qt':getattr(row,'quantity'),'tot':getattr(row,'tot')} ]
            else:
                rqt = getattr(row,'qtPerItem')
                invoice['products']= [ {'pr':id_local+"_"+product_id,'qt':rqt,'tot':getattr(row,'tot')} ]

            num_fact=invoice['number']
            
            rows_0.pop(0)


           
            fail=[]
            for rowitr in rows_0:

               
                if num_fact==getattr(rowitr,'number'):
                    product_id = getattr(rowitr,'product')
                    # IF CHIMBO PARA LOS HELADOS
                    if id_local == "Froyo01" and product_id == '01':
                        invoice['products']= invoice['products'] + [ {'pr':id_local+"_"+product_id,'qt':getattr(rowitr,'quantity'),'tot':getattr(rowitr,'tot')} ]
                    else:
                        rqt = getattr(rowitr,'qtPerItem')
                        invoice['products']= invoice['products'] + [ {'pr':id_local+"_"+product_id,'qt':rqt,'tot':getattr(rowitr,'tot')} ]

                    
                else:
                    
                    package = json.dumps(invoice, cls=DecimalEncoder)                
                    response = sender(config.params["url"],config.params["port"],config.params["invoices"],package,auth)
                    
                    if(response == 600):

                        ri = 0
                        while(ri < 2 and response == 600):

                            response = sender(config.params["url"],config.params["port"],config.params["invoices"],package,auth)
                            ri += 1

                    if not recover(response):
                        fail.append(invoice['number'])

                    qt += 1
                    if qt == progreso:
                        update_progress(porcentaje, qt,n)
                        pivote += 1
                        porcentaje = (100 // 20) * pivote
                        progreso = math.ceil(n / 20) * pivote


                    num_fact=getattr(rowitr,'number')
                    invoice={}
                    invoice['number']= getattr(rowitr,'number')
                    invoice['date']= calendar.timegm(getattr(rowitr,'date').utctimetuple())
                    invoice['client']= getattr(rowitr,'client')
                    if invoice['client'] in config.params['no_id'] :
                        invoice['client']= id_local+'_NOID'
                    
                    invoice['subtotal']= getattr(rowitr,'subtotal')
                    invoice['tax']= getattr(rowitr,'tax')
                    invoice['total']= getattr(rowitr,'total')
                    invoice['local']=id_local

                    product_id = getattr(rowitr,'product')
                    # IF CHIMBO PARA LOS HELADOS
                    if id_local == "Froyo01" and product_id == '01':
                        invoice['products']= [ {'pr':id_local+"_"+product_id,'qt':getattr(rowitr,'quantity'),'tot':getattr(rowitr,'tot')} ]
                    else:
                        rqt = getattr(rowitr,'qtPerItem')
                        invoice['products']= [ {'pr':id_local+"_"+product_id,'qt':rqt,'tot':getattr(rowitr,'tot')} ]
                    
                          
            package = json.dumps(invoice, cls=DecimalEncoder)  
            response = sender(config.params["url"],config.params["port"],config.params["invoices"],package,auth)
            
            if(response == 600):

                ri = 0
                while(ri < 2 and response == 600):

                    response = sender(config.params["url"],config.params["port"],config.params["invoices"],package,auth)
                    ri += 1

            if not recover(response):
                fail.append(invoice['number'])

            qt += 1
            if qt == progreso:
                update_progress(porcentaje, qt,n)
                pivote += 1
                porcentaje = (100 // 20) * pivote
                progreso = math.ceil(n / 20) * pivote

            
            return fail
            
        else:
            logging.info("NO INVOICE RECORDS WILL BE SENT DURING THIS LOAD PROCESS")

            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET INVOICES, EXCEPTION : [%s]",e)
        return None      
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

def get_del_invoices(cursor,id_local,auth):
    
    try:
        if config.params["init"]:
            cursor.execute(" SELECT a.NumeroD as number, a.NumeroR as reference from safact a where a.tipoFac='A' and a.signo=1 and a.numeroR is not null and a.fechaT < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' ")
        else:
            cursor.execute(" SELECT a.NumeroD as number, a.NumeroR as reference from safact a where a.tipoFac='A' and a.signo=1 and a.numeroR is not null and a.fechaT >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' ")
        

        invoices = cursor.fetchall()

        if len(invoices) < 1:

            logging.info("NO DELETED INVOICES RECORDS WILL BE SENT DURING THIS LOAD PROCESS")
            return []


        deletions = []
        origins = []

        for row in invoices:

            deletions.append(getattr(row,'reference'))
            origins.append(getattr(row,'number'))

        queryD = '(' + ', '.join("'{0}'".format(w) for w in deletions) + ')'
        queryO = '(' + ', '.join("'{0}'".format(w) for w in origins) + ')'


        q0_select = ['number','deleted','date','client','subtotal','tax','total']
        
        
        cursor.execute("SELECT Numerod as number, NumeroR as deleted, fechaT as date, id3 as client, Monto as subtotal, mtoTax as tax, mtoTotal as total from safact where numeroR in " + queryO + " and numeroD in " + queryD + " and TipoFac='B'")    
        
        rows = cursor.fetchall()

        fail = []
        if len(rows) > 0 :

            n = len(rows)
            qt = 0
            pivote = 1
            porcentaje = (100 // 20) * pivote
            progreso = math.ceil(n / 20) * pivote
            

            logging.info("%d DELETED INVOICE RECORDS WILL BE SENT DURING THIS LOAD PROCESS",n)

            for row in rows:

                invoices = {}
            
                invoices['number']= getattr(row,'number')
                invoices['deleted']= getattr(row,'deleted')
                invoices['date']= calendar.timegm(getattr(row,'date').utctimetuple())
                invoices['client']= getattr(row,'client')
                if invoices['client'] in config.params['no_id']:
                    invoices['client']= id_local+'_NOID'    
                invoices['subtotal']= getattr(row,'subtotal')
                invoices['tax']= getattr(row,'tax')
                invoices['total']= getattr(row,'total')
                invoices['local']=id_local

                package = json.dumps(invoices, cls=DecimalEncoder)                
                response = sender(config.params["url"],config.params["port"],config.params["cancelinvoices"],package,auth)


                if(response == 600):

                    ri = 0
                    while(ri < 2 and response == 600):

                        response = sender(config.params["url"],config.params["port"],config.params["cancelinvoices"],package,auth)
                        ri += 1

                if not recover(response):
                        fail.append(invoices['number'])

                qt += 1
                if qt == progreso:
                    update_progress(porcentaje, qt,n)
                    pivote += 1
                    porcentaje = (100 // 20) * pivote
                    progreso = math.ceil(n / 20) * pivote

                
            return fail
            
        else:
            logging.info("NO DELETED INVOICES RECORDS WILL BE SENT DURING THIS LOAD PROCESS - WARNING, THIS MESSAGE SHOULDNT APPEAR")

            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET INVOICES, EXCEPTION : [%s]",e)
        return None      
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None


def login(user,passw,id_local):

    authOb = {}
    authOb['email'] = user
    authOb['password'] = passw

    package = json.dumps(authOb,cls=DecimalEncoder)
    #print(package)
    obj = senderBody(config.params["url"],config.params["port"],config.params["login"],package)

    if isinstance(obj,int):
         logging.error("COULD NOT CONNECT TO API, AUTH FAILURE, CODE [%s] ",obj)
    else:
        obj['X-COIN'] = obj.pop('token')
        obj.pop('roles')
        obj.pop('locals')
        obj['ID'] = id_local

    return obj

def make_connection(config_db):
    try:
        logging.debug("DATABASE CONNECTION OPENED")
        cnxn = pyodbc.connect('DRIVER='+config_db['driver']+';SERVER='+config_db['server']+';DATABASE='+config_db['db']+';UID='+config_db['user']+';PWD='+config_db['password'])
        cursor = cnxn.cursor()

    except pyodbc.Error as e: 
        logging.error('COULD NOT CONNECT TO DATABASE SERVER, EXCEPTION : [%s]',e) 
        cursor=None
    return(cursor)

def close_connection(cursor):
    try:
        cursor.close()
        logging.debug('DATABASE CONNECTION CLOSED')

    except pyodbc.Error as e:
        logging.error('COULD NOT CLOSE CONNECTION TO DATABASE SERVER, EXCEPTION : [%s]',e) 
        cursor=None      
    except Exception as e:
        logging.error('SOMETHING WENT WRONG: [%s]',e) 
        cursor=None      


def sender(url,port,endpoint,jsonS,headersD):
    try:
        if not jsonS == None:
            post_data =jsonS.encode('utf-8')
            headers = { 'Content-type': "application/json",
                        'Accept': "application/json"}

            headers.update(headersD)
            request = urllib.request.Request(url+':'+port+endpoint, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        else:
            logging.debug("THERE WAS NOTHING TO SEND IN THIS REQUEST")

        response = 0
    
    except urllib.error.HTTPError as e: 
        if e.getcode() == 403:
            logging.debug("COULD NOT SEND OBJECT, CODE: 403, REASON: UNFORBIDDEN")
            response = e.getcode()    
        else:
            logging.debug("COULD NOT SEND OBJECT, CODE: %s REASON: %s, BODY: %s",e.getcode(),e.reason,e.read().decode('utf8','ignore'))
            response = e.getcode()

    except urllib.error.URLError as e: 
        logging.error("COULD NOT SEND OBJECT, REASON: %s",e.reason)
        response = 600
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG: [%s]',e)
        response = 601
        
    
    return response



def senderBody(url,port,endpoint,jsonS):
    try:
        if not jsonS == None:
            post_data =jsonS.encode('utf-8')
            headers = { 'Content-type': "application/json",
                        'Accept': "application/json"}
            request = urllib.request.Request(url+':'+port+endpoint, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        else:
            logging.debug("THERE WAS NOTHING TO SEND IN THIS REQUEST")

        str_response = body.readall().decode('utf-8')

        if str_response == "":
            response = 0
        else:
            response =  json.loads(str_response)
    
    except urllib.error.HTTPError as e: 
        logging.debug("COULD NOT SEND OBJECT, CODE: %s REASON: %s, BODY: %s",e.getcode(),e.reason,e.read().decode('utf8','ignore'))
        response = e.getcode()

    except urllib.error.URLError as e: 
        logging.error("COULD NOT SEND OBJECT, REASON: %s",e.reason)
        response = 600
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG: [%s]',e)
        response = 601
        
    
    return response


def setConfiguration():

    pp = pprint.PrettyPrinter(indent=4)
    f= open('config.py','w')
    f.write('import datetime \n')
    ppaux = pp.pformat(config.params)
    f.write('params =' +ppaux+'\n')
    f.close()
    f= open('local.py','w')
    ppaux = pp.pformat(local.local)
    f.write('local ='+ppaux+'\n')
    ppaux = pp.pformat(local.configs)
    f.write('configs ='+ppaux+'\n')
    f.close()
    logging.debug("CONFIGURATION WAS SAVED")

def setLogs():

    formatStr = '[%(asctime)s # %(lineno)d # %(levelname)s] %(message)s'
        
    # SE CREA EL HANDLER QUE ESCRIBIRA LA INFORMACION DE FUNCIONAMIENTO DEL PROGRAMA, Y 
    # DE SER EL CASO LOS MENSAJES DE DEBUG
    size = int(config.params['log_size']) * 1048576
    fh = logging.handlers.RotatingFileHandler(config.params['log_file'], maxBytes=size, backupCount=7)
    
    # SE CREA EL HANDLER QUE ENVIARA MENSAJES AL API, SIEMPRE A PARTIR DEL NIVEL INFO PARA NO SOBRECARGAR LA RED
    rh = RESTHandler(config.params["url"],config.params["port"],config.params["errors"])
    rh.setLevel(logging.INFO)

    # SE CREA EL HANDLER QUE ESCRIBIRA EN UN ARCHIVO ROTATORIO CUALQUIER LOG DE ERRORES
    efh = logging.handlers.RotatingFileHandler(config.params['log_error_file'], maxBytes=size, backupCount=7)
    efh.setLevel(logging.ERROR)    

    logging.basicConfig(level='DEBUG',handlers=[fh,rh,efh],format=formatStr)
   

def time_updater():
    if(config.params["init"]):
        config.params["time_init"] = datetime.datetime.now() - datetime.timedelta(minutes=15)
    else:
        config.params["time_load"] = datetime.datetime.now() - datetime.timedelta(minutes=15)

def time_swap():
    if not (config.params["init"]):
        config.params["time_init"]=config.params["time_load"]
    else:
        config.params["init"]=0
   



def main():
    
    

    # SE CONFIGURA Y ACTIVA EL SISTEMA DE LOGS
    setLogs()

    if(config.params["init"]):
        logging.info("DATABASE LOADER STARTED - INITIAL LOAD")
    else:
        logging.info("DATABASE LOADER STARTED - PIVOT [%s]",config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S"))

    # SE ESTABLECE LA CONEXION CON EL SERVIDOR DE BASE DE DATOS
    cursor=make_connection(local.configs)

    auth = login(username,password,local.local['id'])

    # EN CASO DE QUE LA CONEXION CON LA BD SEA EXITOSA
    if not cursor == None and not isinstance(auth,int):
        
        # SE TOMA LA HORA QUE FUNCIONARA DE PIVOTE 
        time_updater()

        # SE EJECUTAN LAS CONSULTAS CORRESPONDIENTES
        data_0 = get_products(cursor,local.local['id'],auth)
        data_1 = get_clients(cursor,local.local['id'],config.params['no_id'],auth)
        data_2 = get_invoices(cursor,local.local['id'],auth)
        data_3 = get_del_invoices(cursor,local.local['id'],auth)
        #print(data_0)
        #print(data_1)
        #print(data_2)
        #print(data_1)
        #print(data_2)

        # SE CIERRA LA CONEXION AL SERVIDOR
        close_connection(cursor)

        # VARIABLE PARA REPETIR EL STRING
        rollback = 0

        if data_0 != [] or data_1 != [] or data_2 != [] or data_3 != []:
            rollback = 1

        
        ## SI NO HUBO ERRORES MANDANDO LAS PETICIONES
        if not rollback:
            ## SE ALMACENA LA HORA PIVOTE PARA LA PROXIMA CONSULTA
            time_swap()
            ## SE GUARDA EL ESTADO DE LAS VARIABLES DE CONFIGURACION EN EL ARCHIVO CORRESPONDIENTE
            setConfiguration()
        else:
            logging.info("SOMETHING WENT WRONG, THE LOADER WILL REPEAT THE PROCESS")

    logging.info("DATABASE LOADER FINISHED - NEXT PIVOT [%s]",config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S"))    


main()    
#except KeyboardInterrupt as e: 
#    print("Lo mataron marik!")    