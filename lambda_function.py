import boto3
import json
import boto3.session
import re
from unicodedata import normalize

client_lambda = boto3.client('lambda')
client = boto3.client("s3")

# Nombre del bucekt donde estan almacenados los videos
bucket_name = "tolsc"

# Obtiene un link "prefirmado" el cual tiene la dirección donde se encuentra almacenado el archivo dentro de S3. Este link solo es valido una hora.
def get_link(name):
    url = boto3.client('s3').generate_presigned_url(
    ClientMethod='get_object', 
    Params={'Bucket': bucket_name, 'Key': name},
    ExpiresIn=3600)
    return url

'''
Solicitar al bucket que liste los diferentes archivos que se tienen almacenados.
se emplea el paginator para obtener la información de todos los archivos sin importar la cantidad de videos.

Por otro lado, en el argumento 'Prefix'  se define de una vez bajo que "carpeta" se va a encontrar los videos.
en este caso, todos los videos siempre van a empezar el key bajo el prefijo "videos/".
En el arreglo "phrase" se retorna el nombre completo del video en caso de que haya coincidencias.
'''
def search_video(text):
    
    try:
        phrase = ''
        paginator = client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name,
                            'Prefix': 'videos/'+text}
                            
        page_iterator = paginator.paginate(**operation_parameters)
        filtered_iterator = page_iterator.search("Contents[?Key][]")
        
        for value in filtered_iterator:
            phrase = value['Key']
            if phrase.replace(".","/").split("/")[1] == text:
                return phrase
                
        return ''
    except:
        return []

# Eliminar las tildes de palabras
def remove_accent(word):
    word = re.sub(
                r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", 
                    normalize( "NFD", word), 0, re.I)
    return word
    
# Funcion que permite el particionamiento de el texto inicial en frases, palabras o letras
def obtaining_phrases(input_text):
    text = input_text
    download=[]
    if len(input_text.split(" ")) > 1:
        
        word_dict = {}
        
        paginator = client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        filtered_iterator = page_iterator.search("Contents[?Key][]")

        for value in filtered_iterator:
            word = value['Key'].replace(".","/").split("/")[1]
            if len(word) > 1:
                word_index = input_text.find(word)
                if  word_index != -1:
                    if word_index==0 or word_index+len(word)==len(input_text):
                        word_dict[word] = word_index
                    elif input_text[word_index-1] == ' ' and input_text[word_index+len(word)] == ' ':
                        word_dict[word] = word_index
        
        if len(word_dict) > 0:                  
            new_dict = {}
            for k in sorted(word_dict, key=len, reverse=True):
                new_dict[k] = word_dict[k]
                
            word_dict = new_dict.copy()
            
            for word in new_dict.keys():
                if text.replace(word,"-") != text: 
                    text = text.replace(word,"-")
                else:
                    del word_dict[word]
            
            text = text.split("-") 
            text = list(filter(lambda x:x!='',text))
            
            for word in text:
                word_dict[word] = input_text.find(word)
        
            word_dict = dict(sorted(word_dict.items(), key = lambda item: item[1]))
                
            for word in word_dict.keys():
                if word in text:
                    word = remove_accent(word)
                    download+=list(word)
                else:
                    download.append(word)
        else:
            download+=list(text)
            
        download = list(filter(lambda x:x!=' ',download))
        download = list(map(search_video, download))
    else:
        text = remove_accent(text)
        download = list(text)
        download = list(map(search_video, download))
        
    return download
    
def lambda_handler(event, context):
    
    print(event)
    
    # Extraer el texto a traducir enviado por el usuario
    input_text = event['queryStringParameters']['files']

    search_response = search_video(input_text)
    if len(search_response) > 0:
        # En caso de encontrar alguna coincidencia, se prodece a llamar la funcion que obtiene el link 
        url = get_link(search_response)
    
        content_type = "text/plain"
        return {
            'statusCode'        : 200,
            'headers'           : { 'Content-Type':content_type  },
            'body'              : url}
            
    # En caso de no encontrar coincidencias   
    else:
        
        # Dividir la palabra o frase en caso de no se encontrada
        download = obtaining_phrases(input_text)
        
        # Creación de los datos necesarios que necesita la función de descarga y union de videos.
        inputParams = {
        "title":input_text,
        "download": download
        }
        
        # Invocación de la funcion lambda que se encarga de obtener los archivos requeridos y unirlos.
        try:
            response = client_lambda.invoke(
                # Cambiar la direccion ARN a la funcion de ensamble creada
                FunctionName = 'arn:assembly:link',
                InvocationType = 'RequestResponse',
                Payload = json.dumps(inputParams)
            )
            
            search_response = search_video(input_text)
            print(search_response)
            url = get_link(search_response)
            
            content_type = "text/plain"
            return {
                'statusCode'        : 200,
                'headers'           : { 'Content-Type':content_type  },
                'body'              : url}
        except:
            content_type = "text/plain"
            response = "Prueba con una frase más corta"
            return {
                'statusCode'        : 504,
                'headers'           : { 'Content-Type':content_type  },
                'body'              : response}
            