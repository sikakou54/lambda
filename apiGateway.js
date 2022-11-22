const AWS = require('aws-sdk');
const apiGateway = new AWS.ApiGatewayManagementApi({
    apiVersion: '2018-11-29',
    endpoint: 'vx92a3mpjf.execute-api.ap-northeast-1.amazonaws.com/production'
});

async function notify(_socketId, _data) {

    let retry = 0;

    while (true) {

        try {

            // コネクション確認
            const connection = await apiGateway.getConnection({
                ConnectionId: _socketId
            }).promise();

            // コネクションがあれば通知する
            if (null !== connection) {
                await apiGateway.postToConnection({
                    Data: JSON.stringify(_data),
                    ConnectionId: _socketId
                }).promise();
            }
            break;

        } catch (e) {

            console.error('notify', _socketId, _data, JSON.stringify(e));

            if (429 === e.statusCode ||
                400 === e.statusCode && 'ThrottlingException' === e.code ||
                503 === e.statusCode && 'ServiceUnavailable' === e.code) {

                // 待機する( リトライ回数 * 10msec )
                await sleep(retry * 10);

            } else {
                break;
            }
        }

        // リトライ回数をインクリメントする
        retry++;
    }

}

/**
 * exports
 */
exports.notify = notify;