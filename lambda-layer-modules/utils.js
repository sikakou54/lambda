
const { CognitoJwtVerifier } = require('aws-jwt-verify');

function sleep(msec) {
    return new Promise(resolve => setTimeout(resolve, msec));
}

function getTimeStamp() {
    return (new Date(Date.now() + ((new Date().getTimezoneOffset() + (9 * 60)) * 60 * 1000))).toLocaleString();
}

function getUtcMsec(offset = 0) {
    return (new Date()).getTime() + offset;
}

async function jwtVerify(jwtToken) {

    let payload = undefined;

    const verifier = CognitoJwtVerifier.create({
        userPoolId: 'ap-northeast-1_NQ7rz6N7T',
        tokenUse: 'id',
        clientId: '2beqljda3gfckjqmhmb7pf44ai',
    });

    try {
        payload = await verifier.verify(jwtToken);
    } catch (e) {
        console.error('jwtVerify', JSON.stringify(e));
    }

    return payload;
}

/**
 * exports
 */
exports.sleep = sleep;
exports.getTimeStamp = getTimeStamp;
exports.getUtcMsec = getUtcMsec;
exports.jwtVerify = jwtVerify;
