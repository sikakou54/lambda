exports.sleep = (msec) => {
    return new Promise(resolve => setTimeout(resolve, msec));
}

exports.getTimeStamp = () => {
    return (new Date(Date.now() + ((new Date().getTimezoneOffset() + (9 * 60)) * 60 * 1000))).toLocaleString();
}

exports.getUtcMsec = (offset) => {
    return (new Date()).getTime() + offset;
}