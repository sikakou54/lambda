exports.progress = {
    none: 'none',
    standby: 'standby',
    ready: 'ready',
    discussion: 'discussion',
    vote: 'vote',
    result: 'result'
};

exports.userState = {
    none: 'none',
    join: 'join',
    standby: 'standby',
    ready: 'ready',
    online: 'online',
    finish: 'finish',
    vote: 'vote',
    votingDone: 'votingDone'
};

exports.userJoinType = {
    positive: 'positive',
    negative: 'negative',
    watcher: 'watcher',
};

exports.userResultType = {
    win: 0,
    lose: 1,
    draw: 2
};

exports.userNorify = {
    notifyStandbyRequest: 'notifyStandbyRequest',
    notifyReadyRequest: 'notifyReadyRequest',
    notifyStartRequest: 'notifyStartRequest',
    notifyVoteRequest: 'notifyVoteRequest',
    notifyResultRequest: 'notifyResultRequest',
    notifyJoinImpossibleRequest: 'notifyJoinImpossibleRequest',
    notifyDiscussionStatus: 'notifyDiscussionStatus'
};
