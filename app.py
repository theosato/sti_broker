# app.py

import os
import boto3
import json
import time

from flask import Flask, jsonify, request
from flask_cors import CORS
from dynamodb_json import json_util as dynamo_json

app = Flask(__name__)
cors = CORS(app)

EVENTS_TABLE = os.environ['EVENTS_TABLE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    client = boto3.client(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    client = boto3.client('dynamodb')
    table = boto3.resource('dynamodb', region_name='us-east-1').Table('events-table-dev')

@app.route("/")
def hello():
    return "Hello World! That's the STI Broker endpoint. Wash your hands and go ahead."

@app.route("/events/<string:event_id>")
def get_event(event_id):
    response = client.get_item(
        TableName=EVENTS_TABLE,
        Key={
            'eventId': { 'S': event_id }
        }
    )
    item = response.get('Item')
    if not item:
        return jsonify({'error': 'Event does not exist'}), 404

    response = {
        'eventId': item.get('eventId').get('S'),
        'eventoNome': item.get('eventoNome').get('S'),
        'eventoData': item.get('eventoData').get('S'),
        'eventoFilas': item.get('eventoFilas').get('M'),
        'eventoServicos': item.get('eventoServicos').get('M'),
        'eventoVisitantes': item.get('eventoVisitantes').get('M')
    }

    return dynamo_json.loads(response)

@app.route("/events", methods=["POST"])
def create_event():
    event_id = request.json.get('eventId')
    evento_nome = request.json.get('eventoNome')
    evento_data = request.json.get('eventoData')
    evento_filas = request.json.get('eventoFilas')
    evento_servicos = request.json.get('eventoServicos')
    evento_visitantes = request.json.get('eventoVisitantes')
    
    if not event_id or not evento_nome:
        return jsonify({'error': 'Please provide eventId and eventoNome'}), 400

    response = client.put_item(
        TableName=EVENTS_TABLE,
        Item={
            'eventId': {'S': event_id },
            'eventoNome': {'S': evento_nome },
            'eventoData': {'S': evento_data },
            'eventoFilas': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) },
            'eventoServicos': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) },
            'eventoVisitantes': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) }
        }
    )

    return jsonify({
        'eventId': {'S': event_id },
        'eventoNome': {'S': evento_nome },
        'eventoData': {'S': evento_data },
        'eventoFilas': {'M': evento_filas },
        'eventoServicos': {'M': evento_servicos },
        'eventoVisitantes': {'M': evento_visitantes }
    })


@app.route("/events/<string:event_id>", methods=["PUT"])
def update_event(event_id):
    event_id = request.json.get('eventId')
    evento_nome = request.json.get('eventoNome')
    evento_data = request.json.get('eventoData')
    evento_filas = request.json.get('eventoFilas')
    evento_servicos = request.json.get('eventoServicos')
    evento_visitantes = request.json.get('eventoVisitantes')
    
    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    response = client.put_item(
        TableName=EVENTS_TABLE,
        Item={
            'eventId': { 'S': event_id },
            'eventoNome': {'S': evento_nome },
            'eventoData': {'S': evento_data },
            'eventoFilas': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) },
            'eventoServicos': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) },
            'eventoVisitantes': {'M': dynamo_json.dumps(dct=evento_filas, as_dict=True) }
        }
    )

    return jsonify({
        'eventId': {'S': event_id },
        'eventoNome': {'S': evento_nome },
        'eventoData': {'S': evento_data },
        'eventoFilas': {'M': evento_filas },
        'eventoServicos': {'M': evento_servicos },
        'eventoVisitantes': {'M': evento_visitantes }
    })

######################################################## FILA ##############################################################

@app.route("/events/<string:event_id>/fila", methods=["PUT"])
def altera_fila(event_id):
    evento_fila = request.get_json(force=True)
    evento_fila["idFila"]=str(evento_fila["idFila"])
    evento_fila["quantPessoas"]=int(evento_fila["quantPessoas"])

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoFilas.#idFila = :valFila",
        ExpressionAttributeNames = {
            "#eventoFilas" : "eventoFilas",
            "#idFila" : evento_fila["idFila"]
        },
        ExpressionAttributeValues={
            ":valFila": evento_fila
        }, 
        ReturnValues="ALL_NEW"
    )
    
    return dynamo_json.loads(response["Attributes"])


@app.route("/events/<string:event_id>/fila/<string:fila_id>/status", methods=["PUT"])
def altera_fila_status(event_id, fila_id):
    evento_fila = request.get_json(force=True)
    # evento_fila["quantPessoas"]=int(evento_fila["quantPessoas"])

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400
    
    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoFilas.#idFila.#filaAtiva = :valFilaAtiva",
        ExpressionAttributeNames = {
            "#eventoFilas" : "eventoFilas",
            "#idFila" : fila_id,
            "#filaAtiva" : "filaAtiva"
        },
        ExpressionAttributeValues={
            ":valFilaAtiva": evento_fila['filaAtiva']
        }, 
        ConditionExpression='attribute_exists(#eventoFilas.#idFila.#filaAtiva)',
        ReturnValues="ALL_NEW"
    )

    return dynamo_json.loads(response["Attributes"])

@app.route("/events/<string:event_id>/fila/<string:fila_id>/visitante/<string:visitante_id>", methods=["PUT"])
def altera_fila_visitante(event_id, fila_id, visitante_id):

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    try:
        response = table.update_item(
            Key={
                "eventId": event_id,
            },
            UpdateExpression="SET #eventoFilas.#idFila.#pessoasFila.#idVisitante = :valTimestamp, \
                              #eventoFilas.#idFila.#quantPessoas = #eventoFilas.#idFila.#quantPessoas + :valQuantPessoas, \
                              #eventoVisitantes.#idVisitante.#visFila = :valIdFila",
            ExpressionAttributeNames = {
                "#eventoFilas" : "eventoFilas",
                "#idFila" : fila_id,
                "#pessoasFila" : "pessoasFila",
                "#idVisitante": visitante_id,
                "#quantPessoas": "quantPessoas",
                "#visFila": "idFila",
                "#eventoVisitantes": "eventoVisitantes"
            },
            ExpressionAttributeValues={
                ":valTimestamp":  str(time.time()),
                ":valQuantPessoas":  1,
                ":valIdFila": fila_id
            },
            ConditionExpression='attribute_not_exists(#eventoFilas.#idFila.#pessoasFila.#idVisitante)',
            ReturnValues="ALL_NEW"
        )

        return dynamo_json.loads(response["Attributes"])


    except:
        response = table.update_item(
            Key={
                "eventId": event_id,
            },
            UpdateExpression="REMOVE #eventoFilas.#idFila.#pessoasFila.#idVisitante \
                              SET #eventoFilas.#idFila.#quantPessoas = #eventoFilas.#idFila.#quantPessoas - :valQuantPessoas, \
                              #eventoVisitantes.#idVisitante.#visFila = :valSemFila",
            ExpressionAttributeNames = {
                "#eventoFilas" : "eventoFilas",
                "#idFila" : fila_id,
                "#pessoasFila" : "pessoasFila",
                "#idVisitante": visitante_id,
                "#quantPessoas": "quantPessoas",
                "#eventoVisitantes": "eventoVisitantes",
                "#visFila": "idFila"
            }, 
            ExpressionAttributeValues={
                ":valQuantPessoas":  1,
                ":valSemFila": "0"
            }, 
            ConditionExpression='attribute_exists(#eventoFilas.#idFila.#pessoasFila.#idVisitante)',
            ReturnValues="ALL_NEW"
        )

        return dynamo_json.loads(response["Attributes"])

@app.route("/events/<string:event_id>/fila/<string:fila_id>/time", methods=["PUT"])
def altera_fila_tempo(event_id, fila_id):
    evento_fila = request.get_json(force=True)
    evento_fila["tempoFila"]=str(evento_fila["tempoFila"])

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400
    
    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoFilas.#idFila.#tempoFila = :valTempoFila",
        ExpressionAttributeNames = {
            "#eventoFilas" : "eventoFilas",
            "#idFila" : fila_id,
            "#tempoFila" : "tempoFila"
        },
        ExpressionAttributeValues={
            ":valTempoFila": evento_fila["tempoFila"]
        }, 
        # ConditionExpression='attribute_exists(#eventoFilas.#idFila.#filaAtiva)',
        ReturnValues="ALL_NEW"
    )

    return dynamo_json.loads(response["Attributes"])

######################################################## CUPOM ##############################################################

@app.route("/events/<string:event_id>/cupom", methods=["PUT"])
def altera_cupom(event_id):
    evento_cupom = request.get_json(force=True)
    evento_cupom["idCupom"]=str(evento_cupom["idCupom"])
    evento_cupom["quantidade"]=int(evento_cupom["quantidade"])
    
    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoServicos.#idCupom = :valCupom",
        ExpressionAttributeNames = {
            "#eventoServicos" : "eventoServicos",
            "#idCupom" : evento_cupom["idCupom"]
        },
        ExpressionAttributeValues={
            ":valCupom": evento_cupom
        },
        ReturnValues="ALL_NEW"
    )

    return dynamo_json.loads(response["Attributes"])

@app.route("/events/<string:event_id>/cupom/<string:cupom_id>/visitante/<string:visitante_id>", methods=["PUT"])
def altera_cupom_visitante(event_id, cupom_id, visitante_id):

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoServicos.#idCupom.#pessoasCupom.#idVisitante = :valTimestamp, \
                            #eventoServicos.#idCupom.#quantidade = #eventoServicos.#idCupom.#quantidade - :valUmCupom, \
                            #eventoVisitantes.#idVisitante.#cuponsUsados.#idCupom = :valTimestamp",
        ExpressionAttributeNames = {
            "#eventoServicos" : "eventoServicos",
            "#idCupom" : cupom_id,
            "#pessoasCupom" : "pessoasCupom",
            "#idVisitante": visitante_id,
            "#quantidade": "quantidade",
            "#cuponsUsados": "cuponsUsados",
            "#eventoVisitantes": "eventoVisitantes"
        },
        ExpressionAttributeValues={
            ":valTimestamp":  str(time.time()),
            ":valUmCupom":  1,
            ":zero": 0
        },
        ConditionExpression="#eventoServicos.#idCupom.#quantidade > :zero",
        ReturnValues="ALL_NEW"
    )

    return dynamo_json.loads(response["Attributes"])

######################################################## VISITANTE ##############################################################

@app.route("/events/<string:event_id>/visitante", methods=["PUT"])
def altera_visitante(event_id):
    evento_visitante = request.get_json(force=True)
    evento_visitante["idVisitante"]=str(evento_visitante["idVisitante"])

    if not event_id:
        return jsonify({'error': 'Please provide eventId'}), 400

    response = table.update_item(
        Key={
            "eventId": event_id,
        },
        UpdateExpression="SET #eventoVisitantes.#idVisitante = :valVisitante",
        ExpressionAttributeNames = {
            "#eventoVisitantes" : "eventoVisitantes",
            "#idVisitante" : evento_visitante["idVisitante"]
        },
        ExpressionAttributeValues={
            ":valVisitante": evento_visitante
        },
        ReturnValues="ALL_NEW"
    )

    return dynamo_json.loads(response["Attributes"])
