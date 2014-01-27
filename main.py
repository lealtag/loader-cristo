import  json,signal, pyodbc, decimal, calendar, datetime, config, urllib.request, urllib.error, sys, logging, logging.handlers, time

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
            data = {'level': record.levelname, 'message': record.getMessage(), "date":record.asctime, "local":config.local['name']}
            jsonS = json.dumps(data, cls=DecimalEncoder)
            headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
            post_data = jsonS.encode('utf-8')
            #print(jsonS)
            request = urllib.request.Request(self.host+':'+self.port+self.url, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        except Exception as e:
            #print(e)
            f = open(config.params["log_error_file"]+'.dead', 'a')
            f.write(time.strftime('%d/%m/%Y %H:%M:%S')+' # CONNECTION ERROR # '+self.host+':'+self.port+self.url+'\n')
            f.close()
#            

def recover(code):
    if code > 0 and code != 422:
        return False
    return True

    


def get_products(cursor,id_local):
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

            logging.info("%d PRODUCT RECORDS WILL BE SENT DURING THIS LOAD PROCESS",len(rows))

            fail = []

            for row in rows:
                product = {}
        
                for i in range(len(q0_select)):
                    #print(type(getattr(row,q0_select[i])))
                    product[q0_select[i]] = getattr(row,q0_select[i])
                
                product['_id']=id_local+'_'+product['code']
                product['local']=id_local
        
                #products = products + [product,]
                package = json.dumps(product, cls=DecimalEncoder)                
                response = sender(config.params["url"],config.params["port"],config.params["products"],package)
                if not recover(response):
                    fail.append(product['_id'])
                
            return fail

            #to_json={}
            #to_json['products']=products
            
        
            #return(json.dumps(to_json, cls=DecimalEncoder))
        else:

            logging.info("NO PRODUCT RECORDS WILL BE SENT DURING THIS LOAD PROCESS ")
            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET PRODUCTS, EXCEPTION : [%s]",e)
        return None     
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

def get_clients(cursor,id_local,no_id):  
    try:
        q0_select = ['_id','name','address']
        if config.params["init"]:
            cursor.execute("SELECT codClie as _id, Descrip as name, direc1 as address from saclie where fechaUV is not null AND fechaUV < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")    
        else:
            cursor.execute("SELECT codClie as _id, Descrip as name, direc1 as address from saclie where fechaUV is not null AND fechaUV >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\'")   
    
        rows = cursor.fetchall()
        #print(rows)
        #print(json.dump(rows))
        if len(rows) > 0 :

            logging.info("%d CLIENT RECORDS WILL BE SENT DURING THIS LOAD PROCESS",len(rows))

            fail = []
            for row in rows:
                client = {}
        
                for i in range(len(q0_select)):
                    #print(type(getattr(row,q0_select[i])))
                    client[q0_select[i]] = getattr(row,q0_select[i])
                
                for name in no_id:
                    
                    if client['_id']==name:
                        client['_id']=id_local+'_NOID'
                
        
                #clients = clients + [client,]
                package = json.dumps(client, cls=DecimalEncoder)                
                response = sender(config.params["url"],config.params["port"],config.params["clients"],package)
                if not recover(response):
                    fail.append(client['_id'])
            #to_json={}
            #to_json['clients']=clients
            return fail
            
            #return(json.dumps(to_json, cls=DecimalEncoder))
        else:
            logging.info("NO CLIENT RECORDS WILL BE SENT DURING THIS LOAD PROCESS")

            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET CLIENTS, EXCEPTION : [%s]",e)
        return None
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

def get_invoices(cursor,id_local):
    
    try:
        q0_select = ['number','date','client','subtotal','tax','total', 'product', 'quantity']
        if config.params["init"]:
            cursor.execute(" SELECT a.Numerod as number, a.fechaT as date, a.id3 as client, a.monto as subtotal, a.mtoTax as tax, a.mtoTotal as total, b.codItem as product, count(b.codItem) as quantity from safact a , saitemfac b where a.tipoFac='A' and a.signo=1 and a.numeroD=b.numeroD and a.fechaT < \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' group by a.Numerod, a.fechaT, a.id3, a.monto, a.mtoTax, a.mtoTotal, b.codItem order by a.Numerod")
        else:
            cursor.execute(" SELECT a.Numerod as number, a.fechaT as date, a.id3 as client, a.monto as subtotal, a.mtoTax as tax, a.mtoTotal as total, b.codItem as product, count(b.codItem) as quantity from safact a , saitemfac b where a.tipoFac='A' and a.signo=1 and a.numeroD=b.numeroD and a.fechaT >= \'"+config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S") +"\' group by a.Numerod, a.fechaT, a.id3, a.monto, a.mtoTax, a.mtoTotal, b.codItem order by a.Numerod")
        
        rows_0 = cursor.fetchall()
        if len(rows_0) > 0 :

            logging.info("%d INVOICE RECORDS WILL BE SENT DURING THIS LOAD PROCESS",len(rows_0))

            #print(rows_0)
            #print(json.dump(rows_0))
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
        
        
            invoice['products']= [ {'pr':id_local+"_"+getattr(row,'product'),'qt':getattr(row,'quantity')} ]
            
            num_fact=invoice['number']
            rows_0.pop(0)
           
            fail=[]
            for rowitr in rows_0:
              
                if num_fact==getattr(rowitr,'number'):
                    invoice['products']= invoice['products'] +[ {'pr':id_local+'_'+getattr(rowitr,'product'),'qt':getattr(rowitr,'quantity')},]
                else:
                    #invoices = invoices + [invoice,]
                    package = json.dumps(invoice, cls=DecimalEncoder)                
                    response = sender(config.params["url"],config.params["port"],config.params["invoices"],package)
                    if not recover(response):
                        fail.append(invoice['number'])


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

        
                    invoice['products']= [ {'pr':id_local+"_"+getattr(rowitr,'product'),'qt':getattr(rowitr,'quantity')} ]
                          
            #invoices = invoices + [invoice,]
            package = json.dumps(invoice, cls=DecimalEncoder)                
            response = sender(config.params["url"],config.params["port"],config.params["invoices"],package)
            if not recover(response):
                fail.append(invoice['number'])
            #to_json={}
            #to_json['invoices']=invoices
        
            return fail
            #return(json.dumps(to_json, cls=DecimalEncoder))
        else:
            logging.info("NO INVOICE RECORDS WILL BE SENT DURING THIS LOAD PROCESS")

            return []
    except pyodbc.Error as e:
        logging.error("COULD NOT EXECUTE QUERY TO GET INVOICES, EXCEPTION : [%s]",e)
        return None      
    except Exception as e:
        logging.error("SOMETHING WENT WRONG, EXCEPTION : [%s]",e)
        return None

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

def sender(url,port,endpoint,json):
    try:
        if not json == None:
            post_data =json.encode('utf-8')
            headers = { 'Content-type': "application/json",
                        'Accept': "application/json"}
            request = urllib.request.Request(url+':'+port+endpoint, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        else:
            logging.debug("THERE WAS NOTHING TO SEND IN THIS REQUEST")

        response = 0
    
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

    f= open('config.py','w')
    f.write('import datetime \n')
    f.write('local ='+str(config.local)+'\n')
    f.write('configs ='+str(config.configs)+'\n')
    f.write('params =' +str(config.params)+'\n')
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
   

def main():
    
    

    # SE CONFIGURA Y ACTIVA EL SISTEMA DE LOGS
    setLogs()

    if(config.params["init"]):
        logging.info("DATABASE LOADER STARTED - INITIAL LOAD")
    else:
        logging.info("DATABASE LOADER STARTED - PIVOT [%s]",config.params["time_init"].strftime("%Y-%m-%d %H:%M:%S"))

    # SE ESTABLECE LA CONEXION CON EL SERVIDOR DE BASE DE DATOS
    cursor=make_connection(config.configs)

    # EN CASO DE QUE LA CONEXION CON LA BD SEA EXITOSA
    if not cursor == None:
        
        # SE TOMA LA HORA QUE FUNCIONARA DE PIVOTE 
        time_updater()

        # SE EJECUTAN LAS CONSULTAS CORRESPONDIENTES
        data_0 = get_products(cursor,config.local['id'])
        data_1 = get_clients(cursor,config.local['id'],config.params['no_id'])
        data_2 = get_invoices(cursor,config.local['id'])
        #print(data_0)
        #print(data_1)
        #print(data_2)
        #print(data_1)
        #print(data_2)

        # SE CIERRA LA CONEXION AL SERVIDOR
        close_connection(cursor)

        # VARIABLE PARA REPETIR EL STRING
        rollback = 0

        if data_0 != [] or data_1 != [] or data_2 != []:
            rollback = 1

        #SE ENVIAN LOS RESULTADOS DE LAS CONSULTAS 
        #response = sender(config.params["url"],config.params["port"],config.params["products"],data_0)
        #if response == None:
            #rollback = 1


        #response = sender(config.params["url"],config.params["port"],config.params["clients"],data_1)
        #if response == None:
            #rollback = 1

        #response = sender(config.params["url"],config.params["port"],config.params["invoices"],data_2)
        #if response == None:
            #rollback = 1

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