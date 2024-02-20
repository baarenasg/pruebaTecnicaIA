import boto3
from Crypto.Hash import MD5
from datetime import datetime

#se debe manejar mediante un secreto o variables de ambiente, en la lambda se deben otorgar permisos en IAM
secret = {
'access-key':"",
    'secret-access-key':""
}

bucket_name =  "ai-technical-test-barenas"
dynamodb_table = "ai-technical-testbarenas"

def hashString(data_dict):
    """
    input: Diccionario con los datos del archivo de texto
    Descripción: Resive un diccionario y retorna un hash MD5 de acuerdo con el formato de la cadena definido
    return: Hash MD5 de la cadena procesada
    """
    try:
        sring_to_hash = r"{totalContactoClientes}~{motivoReclamo}~{motivoGarantia}~{motivoDuda}~{motivoCompra}~{motivoFelicitaciones}~{motivoCambio}".format(
        totalContactoClientes = data_dict["totalContactoClientes"],
        motivoReclamo = data_dict["motivoReclamo"],
        motivoGarantia = data_dict["motivoGarantia"],
        motivoDuda = data_dict["motivoDuda"],
        motivoCompra = data_dict["motivoCompra"],
        motivoFelicitaciones = data_dict["motivoFelicitaciones"],
        motivoCambio = data_dict["motivoCambio"])
    
        hash_object = MD5.new()
        hash_object.update(sring_to_hash.encode())
        hex_dig = hash_object.hexdigest()
    except Exception as error:
        print("Error generando el hash", error)
    return hex_dig

def validateHash(hashFromText,hashFromDict):
    """
    input: hassh del archivo de texto, hash obtenido mediante los datos procesados
    Permite validar si el Hash obtenido del archivo de text de la S3 es igual al objetenido mediante los datos procesados
    return: True si son iguales, False si son diferentess
    """
    if hashFromText == hashFromDict:
        return True
    else:
        return False
    
def objectToDict(obj):
    """
    input: Objecto de la S3
    Descrpción: Convierte el objeto de la S3 a un diccionario procesando cada linea del archivo de texto 
    return: Diccionario con los datos del archivo de texto
    """

    data = obj.get()['Body'].readlines()
    data_dict = {}
    for line in data:
        line = str(line).replace("b\'","").replace("\\r\\n\'","").replace("\'","")
        key, value = line.split("=")[0], line.split("=")[1]
        data_dict[key] = value
    return data_dict

def getObjectFromBucket(bucket_name,event):
    """
    input: bucket de la S3
    Descripción: Obtiene el primer objeto del bucket (Supone que solo hay un objeto en el bucket)
    return: Objecto de la S3
    """

    query_result_object_key = event["Records"][0]['s3']['object']['key']
    
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=query_result_object_key)
    return obj 

def lambdaMain(bucket_name,dynamodb_table):
    """
    input: Nombre del bucket de la S3, Nombre de la tabla de la DynamoDB
    Descripción: Función principal que se ejecuta en la lambda, obtiene el objeto de la S3, lo convierte a un diccionario, lo procesa y lo inserta en la DynamoDB, luego elimina el objeto de la S3
    return: Mensaje de éxito o error
    """

    ###Esto se puede ajustar mediante variables de ambiente
    session = boto3.Session(
        aws_access_key_id=secret['access-key'],
        aws_secret_access_key=secret['secret-access-key'],
    )
    
    obj = getObjectFromBucket(bucket_name,dynamodb_table)
    data_dict = objectToDict(obj)

    hashFromDict = hashString(data_dict)

    if validateHash(data_dict["hash"],hashFromDict):
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(dynamodb_table)
        try:
            data_dict["timestamp"] = datetime.now().strftime("%m-%d-%Y_%H:%M:%S")
            response = table.put_item(Item=data_dict)
        except Exception as error: 
            print("Error insertando los datos", error) 

        try:
            obj.delete()
            return "Objeto eliminado correctamente"
        except Exception as error: 
            print("Error eliminando el objeto", error) 
    else:
        return "Error: El hash no coincide"

def lambda_handler(event, context):
    """
    Función que se ejecuta en la lambda, recibe el evento y el contexto	
    """
    return lambdaMain(bucket_name,dynamodb_table)