import psycopg2
import pymysql
import sys
import datetime
import gc

try:
    conPG = psycopg2.connect(host='localhost', database='producao',user='root', password='root') 
    cPG = conPG.cursor()
except Exception as e:
    print('Exception com o Postgress'+e)

try:
    # conMY = pymysql.connect(host="192.168.0.69",user="admin_python",password="abc1936",database="admin_python")
    conMY = pymysql.connect(host="localhost",user="root",password="",database="test")
    cMY = conMY.cursor()
    conMY.autocommit(True)
except Exception as e: 
    print('Exception com o MySql'+e)

print("Abriu Conexao dos 2 Bancos")
SQLschema = 'SELECT schema FROM comum.empresa where comum.empresa.ativado = true ' 
print('vai execultar o sql dos esquemas')
cPG.execute(SQLschema)
print('execulto sql dos esquemas')
schemas = cPG.fetchall()
print('tranformou em array os esquemas')
sqlcru = 'select linha.status,linha.texto,linha.fatura_id,fatura.ano,fatura.mes from esquema.linha inner join esquema.fatura on linha.fatura_id = fatura.id where erro is null and fatura.status = \'APROVADA\' AND linha.status=\'PROCESSADO\' and fatura.ano > 2017 order by fatura.ano desc'
sqlcrufaturas = 'select distinct(fatura_id) from esquema.linha order by fatura_id desc '
sqlinsert = 'INSERT INTO EXTRAIDO_SGT VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
print("Limpou a Tabela")
cMY.execute("DELETE FROM EXTRAIDO_SGT")
print('vai entrar no For do satanas')

arrInsert = []



for x in schemas: 
    print('primeira linha do for replace de faturas id')       
    cPG.execute(sqlcrufaturas.replace('esquema',str(x[0])))  
    sqlfaturaid = cPG.fetchall() 
    print('while')  
    i = 0
    if(len(sqlfaturaid) != 0):        
        while i < len(sqlfaturaid):        
            sql = sqlcru.replace('esquema',str(x[0]))        
            sql = sql.format(str(sqlfaturaid[i][0]))
            print('Empresa '+str(x[0])+'')
            print('sql montado vai para a execucao na linha 2 do for --- '+sql+' ---')
            startTime = datetime.datetime.today ()            
            cPG.execute(sql)
            endTime = datetime.datetime.today ()
            print ( " Encerrado ... em " , endTime - startTime )
            print('Executo o sql') 
            fatura = cPG.fetchall()
            print('Monto o Array do resultado sql') 
            print('Empresa '+str(x[0])+'')
            print(len(fatura))

            if(len(fatura) != 0): 
                startTime = datetime.datetime.today ()   
                for y in fatura: 
                    info = y[1].split(";")                    
                    # arrInsert.append((info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8],info[9], info[10], info[11], info[12], info[13], info[14], info[15], info[16], info[17], info[18], info[19],info[20],info[21],info[22],info[23],int(y[2]),int(y[3]),int(y[4])))                    
                    arrInsert.append((info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8],info[9], info[10], info[11], info[12], info[13], info[14], info[15], info[16], info[17], info[18], info[19],info[20],info[21],info[22],str(y[2]),str(y[3]),str(y[4])))
                    if len(arrInsert) == 5:
                        print(arrInsert)
                        cMY.executemany(sqlinsert, arrInsert)
                        del arrInsert
                        gc.collect()
                        arrInsert = []
                    # a = 'INSERT INTO EXTRAIDO_SGT VALUES (\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\',\'{6}\',\'{7}\',\'{8}\',\'{9}\',\'{10}\',\'{11}\',\'{12}\',\'{13}\',\'{14}\',\'{15}\',\'{16}\',\'{17}\',\'{18}\',\'{19}\',\'{20}\',null,null,null,null,null,{21},{22},{23})'.format(info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8],info[9], info[10], info[11], info[12], info[13], info[14], info[15], info[16], info[17], info[18], info[19], info[20],int(y[2]),int(y[3]),int(y[4]))
                    #print(a)
                    #cMY.execute(a)
                    
            i= i+1

if len(arrInsert) > 0:
     cMY.executemany(sqlinsert, arrInsert)

endTime = datetime.datetime.today ()
print ( " Insert de "+len(fatura)+" linhas encerrado ... em " , endTime - startTime )
    
conPG.close()
conMY.close()
print("Foi Tudo----------------------")