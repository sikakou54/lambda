const AWS = require('aws-sdk');
const dynamo = new AWS.DynamoDB.DocumentClient();
const { sleep } = require('./utils');

const type = {
    put: 0,
    update: 1,
    delete: 2,
    query: 3,
    get: 4,
    transWrite: 5,
    transGet: 6
};

function isDynamoDbRetry(statusCode, code) {

    let result = false;

    // 400系エラーの場合
    if (400 === statusCode) {

        // スループット系スロットリング系のエラーの場合はスリープ後にリトライする
        if ('ProvisionedThroughputExceeded' === code ||
            'ProvisionedThroughputExceededException' === code ||
            'RequestLimitExceeded' === code ||
            'ThrottlingException' === code) {

            // リトライする
            result = true;
        }

        // 503系エラーの場合
    } else if (503 === statusCode) {

        // リトライする
        result = true;

    } else {

        // リトライしない
        result = false;
    }

    return result;
}

async function dynamoDbWrapper(_type, _params) {

    let data = null;

    switch (_type) {

        case type.put:
            data = await dynamo.put(_params).promise();
            break;

        case type.update:
            data = await dynamo.update(_params).promise();
            break;

        case type.delete:
            data = await dynamo.delete(_params).promise();
            break;

        case type.query:
            data = await dynamo.query(_params).promise();
            break;

        case type.get:
            data = await dynamo.get(_params).promise();
            break;

        case type.transWrite:
            data = await dynamo.transactWrite({ TransactItems: _params }).promise();
            break;

        case type.transGet:
            data = await dynamo.transactGet({ TransactItems: _params }).promise();
            break;

        default:
            break;
    }

    return data;
}

async function dynamoHandler(_type, _params) {

    let retry = 0;
    let obj = {
        result: false,
        data: null,
        error: null
    };

    while (true) {

        try {

            // update
            const data = await dynamoDbWrapper(_type, _params);

            // 戻り値を設定する
            obj.result = true;
            obj.data = data;
            obj.error = null;

            // リトライしない
            break;

        } catch (e) {

            console.log(e);

            obj.result = false;
            obj.data = null;
            obj.error = e;

            if (isDynamoDbRetry(e.statusCode, e.code)) {

                // リトライ回数 * 10msec 待機する
                await sleep(retry * 10);

            } else {

                // リトライしない
                break;
            }

        }

        // リトライ回数を加算する
        retry++;
    }

    return obj;
}

async function Put(_params) {
    return await dynamoHandler(type.put, _params);
}

async function Delete(_params) {
    return await dynamoHandler(type.delete, _params);
}

async function Update(_params) {
    return await dynamoHandler(type.update, _params);
}

async function Query(_params) {
    return await dynamoHandler(type.query, _params);
}

async function Get(_params) {
    return await dynamoHandler(type.get, _params);
}

async function TransWrite(_params) {
    return await dynamoHandler(type.transWrite, _params);
}

async function TransGet(_params) {
    return await dynamoHandler(type.transGet, _params);
}

/**
 * exports
 */
exports.Put = Put;
exports.Delete = Delete;
exports.Update = Update;
exports.Query = Query;
exports.Get = Get;
exports.TransWrite = TransWrite;
exports.TransGet = TransGet;