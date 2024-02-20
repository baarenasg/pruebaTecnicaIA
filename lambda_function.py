import boto3
from Crypto.Hash import MD5
from datetime import datetime

secret = {
'access-key':"AKIATIFHXA5QERQXNW5L",
    'secret-access-key':"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"
}
bucket_name =  "ai-technical-test-barenas"
dynamodb_table = "ai-technical-testbarenas"


def hashString(data_dict):
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
    return hex_dig

def validateHash(hashFromText,hashFromDict):
    if hashFromText == hashFromDict:
        return True
    else:
        return False
    


# Create a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id=secret['access-key'],
    aws_secret_access_key=secret['secret-access-key'],
)

# Get the S3 resource from the session
s3 = session.resource('s3')

# Now you can access your bucket
bucket = s3.Bucket(bucket_name)

# For example, to list all the objects in the bucket
for obj in bucket.objects.all():
    print(obj.key)

data = obj.get()['Body'].readlines()
data_dict = {}
for line in data:
    line = str(line).replace("b\'","").replace("\\r\\n\'","").replace("\'","")
    key, value = line.split("=")[0], line.split("=")[1]
    data_dict[key] = value

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
    except Exception as error: 
        print("Error eliminando el objeto", error) 
