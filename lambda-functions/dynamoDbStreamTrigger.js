const { Query, Put, Delete } = require('/opt/dynamoDb');
const { setDiscussionLimitTime,
    setDiscussionProgress,
    setDiscussionMeeting,
    setDiscussionResult,
    deleteDiscussionMeeting,
    getDiscussionLimitTime,
    getDiscussionResult,
    getDiscussionMeetingConfig,
    setDiscussionPub } = require('/opt/discussion');
const { notify } = require('/opt/apiGateway');
const { getUtcMsec } = require('/opt/utils');
const { progress, userState, userJoinType, userNorify } = require('/opt/define');

const userNotifyTable = [

    /******************************************************************************************************************************************************************
     * standby
     ******************************************************************************************************************************************************************/

    /* -> none */
    { preProgress: progress.none, nextProgress: progress.none, preState: userState.none, nextState: userState.join, notify: userNorify.notifyJoinImpossibleRequest },
    { preProgress: progress.standby, nextProgress: progress.none, preState: userState.none, nextState: userState.join, notify: userNorify.notifyStandbyRequest },

    /* -> standby */
    { preProgress: progress.standby, nextProgress: progress.standby, preState: userState.none, nextState: userState.join, notify: userNorify.notifyStandbyRequest },

    /* -> ready */
    { preProgress: progress.standby, nextProgress: progress.ready, preState: userState.none, nextState: userState.join, notify: userNorify.notifyReadyRequest },
    { preProgress: progress.standby, nextProgress: progress.ready, preState: userState.join, nextState: userState.standby, notify: userNorify.notifyReadyRequest },
    { preProgress: progress.standby, nextProgress: progress.ready, preState: userState.standby, nextState: userState.standby, notify: userNorify.notifyReadyRequest },

    /******************************************************************************************************************************************************************
     * ready
     ******************************************************************************************************************************************************************/

    /* -> ready */
    { preProgress: progress.ready, nextProgress: progress.ready, preState: userState.none, nextState: userState.join, notify: userNorify.notifyReadyRequest },

    /* -> standby */
    { preProgress: progress.ready, nextProgress: progress.standby, preState: userState.none, nextState: userState.join, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.ready, nextProgress: progress.standby, preState: userState.join, nextState: userState.online, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.ready, nextProgress: progress.standby, preState: userState.standby, nextState: userState.standby, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.ready, nextProgress: progress.standby, preState: userState.standby, nextState: userState.online, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.ready, nextProgress: progress.standby, preState: userState.online, nextState: userState.online, notify: userNorify.notifyStandbyRequest },

    /* -> discussion */
    { preProgress: progress.ready, nextProgress: progress.discussion, preState: userState.none, nextState: userState.join, notify: userNorify.notifyReadyRequest },
    { preProgress: progress.ready, nextProgress: progress.discussion, preState: userState.join, nextState: userState.online, notify: userNorify.notifyStartRequest },
    { preProgress: progress.ready, nextProgress: progress.discussion, preState: userState.standby, nextState: userState.online, notify: userNorify.notifyStartRequest },
    { preProgress: progress.ready, nextProgress: progress.discussion, preState: userState.online, nextState: userState.online, notify: userNorify.notifyStartRequest },

    /******************************************************************************************************************************************************************
     * discussion
     ******************************************************************************************************************************************************************/

    /* -> standby */
    { preProgress: progress.discussion, nextProgress: progress.standby, preState: userState.standby, nextState: userState.online, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.discussion, nextProgress: progress.standby, preState: userState.online, nextState: userState.online, notify: userNorify.notifyStandbyRequest },
    { preProgress: progress.discussion, nextProgress: progress.standby, preState: userState.online, nextState: userState.finish, notify: userNorify.notifyStandbyRequest },

    /* -> discussion */
    { preProgress: progress.discussion, nextProgress: progress.discussion, preState: userState.none, nextState: userState.join, notify: userNorify.notifyReadyRequest },
    { preProgress: progress.discussion, nextProgress: progress.discussion, preState: userState.join, nextState: userState.online, notify: userNorify.notifyStartRequest },
    { preProgress: progress.discussion, nextProgress: progress.discussion, preState: userState.standby, nextState: userState.online, notify: userNorify.notifyStartRequest },

    /* -> vote */
    { preProgress: progress.discussion, nextProgress: progress.vote, preState: userState.none, nextState: userState.join, notify: userNorify.notifyJoinImpossibleRequest },
    { preProgress: progress.discussion, nextProgress: progress.vote, preState: userState.online, nextState: userState.finish, notify: userNorify.notifyVoteRequest },
    { preProgress: progress.discussion, nextProgress: progress.vote, preState: userState.finish, nextState: userState.finish, notify: userNorify.notifyVoteRequest },

    /******************************************************************************************************************************************************************
     * vote
     ******************************************************************************************************************************************************************/

    /* -> vote */
    { preProgress: progress.vote, nextProgress: progress.vote, preState: userState.none, nextState: userState.join, notify: userNorify.notifyJoinImpossibleRequest },

    /* -> result */
    { preProgress: progress.vote, nextProgress: progress.result, preState: userState.none, nextState: userState.join, notify: userNorify.notifyJoinImpossibleRequest },
    { preProgress: progress.vote, nextProgress: progress.result, preState: userState.finish, nextState: userState.finish, notify: userNorify.notifyResultRequest },
    { preProgress: progress.vote, nextProgress: progress.result, preState: userState.finish, nextState: userState.votingDone, notify: userNorify.notifyResultRequest },
    { preProgress: progress.vote, nextProgress: progress.result, preState: userState.votingDone, nextState: userState.votingDone, notify: userNorify.notifyResultRequest },

    /******************************************************************************************************************************************************************
     * result
     ******************************************************************************************************************************************************************/

    /* -> result */
    { preProgress: progress.result, nextProgress: progress.result, preState: userState.none, nextState: userState.join, notify: userNorify.notifyJoinImpossibleRequest }
];
const watchersMax = 3;
const discussionTimeLimit = 600000;
const voteTimeLimit = 30000;

function checkStandby(_image) {

    let positive = _image.users.filter((v) => v.type === userJoinType.positive);
    let negative = _image.users.filter((v) => v.type === userJoinType.negative);
    let watchers = _image.users.filter((v) => v.type === userJoinType.watcher);
    let watchersStandby = _image.users.filter((v) => v.type === userJoinType.watcher && v.state === userState.standby);

    // 以下条件を満たす場合、準備中に遷移する
    // 肯定と否定が「参加」or「待機中」の場合
    // 「参加」or「待機中」の視聴者数が３人以上の場合
    if ('none' !== positive[0].userId && 'none' !== negative[0].userId && watchersMax <= watchers.length && watchers.length <= watchersStandby.length) {

        // 準備中に遷移する
        return progress.ready;

    } else {

        // 待機中にとどまる
        return progress.standby;
    }
}

function checkReady(_image) {

    let positive = _image.users.filter((v) => v.type === userJoinType.positive);
    let negative = _image.users.filter((v) => v.type === userJoinType.negative);
    let watchers = _image.users.filter((v) => v.type === userJoinType.watcher);

    // 開始条件に満たない場合は準備中に戻る
    if ('none' !== positive[0].userId && 'none' !== negative[0].userId && watchersMax <= watchers.length) {

        positive = _image.users.filter((v) => v.type === userJoinType.positive && v.state === userState.online);
        negative = _image.users.filter((v) => v.type === userJoinType.negative && v.state === userState.online);
        let watchersOnline = _image.users.filter((v) => v.type === userJoinType.watcher && v.state === userState.online);

        // 以下条件を満たす場合、「準備中」にとどまる。
        // 肯定と否定が「オンライン」の場合
        // 「オンライン」の視聴者数が３人以上の場合
        if (0 < positive.length && 0 < negative.length && watchers.length <= watchersOnline.length) {

            // 討論中に遷移する
            return progress.discussion;

        } else {
            // 準備中にとどまる
            return progress.ready;
        }

    } else {

        // 待機中に遷移する
        return progress.standby;
    }
}

function checkDiscussion(_image) {

    let positive = _image.users.filter((v) => v.type === userJoinType.positive);
    let negative = _image.users.filter((v) => v.type === userJoinType.negative);
    let watchers = _image.users.filter((v) => v.type === userJoinType.watcher);

    // 開始条件に満たない場合は準備中に戻る
    if ('none' !== positive[0].userId && 'none' !== negative[0].userId && watchers.length >= watchersMax) {

        positive = _image.users.filter((v) => v.type === userJoinType.positive && v.state === userState.finish);
        negative = _image.users.filter((v) => v.type === userJoinType.negative && v.state === userState.finish);
        let finishWatchers = _image.users.filter((v) => v.type === userJoinType.watcher && v.state === userState.finish);

        if (0 < positive.length && 0 < negative.length && finishWatchers.length === watchers.length) {

            // 投票中に遷移する
            return progress.vote;

        } else {
            // 討論中にとどまる
            return progress.discussion;
        }

    } else {

        // 待機中に遷移する
        return progress.standby;
    }
}

function checkVote(_image) {

    let positive = _image.users.filter((v) => v.type === userJoinType.positive);
    let negative = _image.users.filter((v) => v.type === userJoinType.negative);
    let watchers = _image.users.filter((v) => v.type === userJoinType.watcher);

    // 開始条件に満たない場合は準備中に戻る
    if ('none' !== positive[0].userId || 'none' !== negative[0].userId || watchers > 0) {

        let votingDoneWatchers = _image.users.filter((v) => v.type === userJoinType.watcher && v.state === userState.votingDone);

        // 以下条件を満たす場合、「結果発表中」に遷移する
        // 全ての視聴者が「投票完了」の場合
        if (watchers.length === votingDoneWatchers.length) {

            // 結果発表中に遷移する
            return progress.result;

        } else {
            // 投票中にとどまる
            return progress.vote;
        }

    } else {

        // 待機中に遷移する
        return progress.standby;
    }
}

function checkResult(_image) {

    let positive = _image.users.filter((v) => v.type === userJoinType.positive);
    let negative = _image.users.filter((v) => v.type === userJoinType.negative);
    let watchers = _image.users.filter((v) => v.type === userJoinType.watcher);

    // 以下条件を満たす場合、「待機中」に遷移する
    // 肯定と否定が「状態なし」の場合
    // 視聴者数が0(視聴者が全員抜けた)場合
    if ('none' === positive[0].userId && 'none' === negative[0].userId && 0 === watchers.length) {

        // 待機中に遷移する
        return progress.none;

    } else {

        //  結果発表中にとどまる
        return progress.result;
    }
}

function checkProgress(_image) {

    let next = _image.progress;

    switch (_image.progress) {

        case progress.none:
            break;

        case progress.standby:
            next = checkStandby(_image);
            break;

        case progress.ready:
            next = checkReady(_image);
            break;

        case progress.discussion:
            next = checkDiscussion(_image);
            break;

        case progress.vote:
            next = checkVote(_image);
            break;

        case progress.result:
            next = checkResult(_image);
            break;

        default:
            console.log('progress not found..', _image);
            break;
    }

    return next;
}

async function entryNone(_postId, _progress, _users) {
    await setDiscussionPub(_postId, false);
}

async function entryStandby(_postId, _progress, _users) {

    // 進捗キャッシュを初期化
    await deleteProgressCacheTable(_postId);

    // ユーザー状態遷移キャッシュを初期化
    await deleteUserCacheTable(_postId);

    await setDiscussionLimitTime(_postId, 0);

    await deleteDiscussionMeeting(_postId);
}

async function entryReady(_postId, _progress, _users) {
    await setDiscussionMeeting(_postId);
}

async function entryDiscussion(_postId, _progress, _users) {
    await setDiscussionLimitTime(_postId, getUtcMsec(discussionTimeLimit));
}

async function entryVote(_postId, _progress, _users) {
    await setDiscussionLimitTime(_postId, getUtcMsec(voteTimeLimit));
}

async function entryResult(_postId, _progress, _users) {
    await setDiscussionResult(_postId, _progress, _users);
}

async function exitDiscussion(_postId, _progress, _users) {
    await setDiscussionLimitTime(_postId, 0);
    await deleteDiscussionMeeting(_postId);
}

async function deleteProgressCacheTable(_postId) {

    await Delete({
        TableName: 'progressCacheTable',
        Key: {
            postId: _postId
        }
    });
}

async function getUserCacheTable(_postId) {

    let sockets = [];

    const { data } = await Query({
        TableName: 'userCacheTable',
        KeyConditionExpression: '#postId = :postId',
        ExpressionAttributeNames: {
            '#postId': 'postId'
        },
        ExpressionAttributeValues: {
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        for (let i = 0; i < data.Items.length; i++) {
            sockets.push({
                socketId: data.Items[i].socketId
            });
        }
    }

    return sockets;
}

async function deleteUserCacheTable(_postId) {

    const sockets = await getUserCacheTable(_postId);

    for (let i = 0; i < sockets.length; i++) {
        await Delete({
            TableName: 'userCacheTable',
            Key: {
                postId: _postId,
                socketId: sockets[i].socketId
            }
        });
    }

}

async function exitResult(_postId, _progress, _users) {
    //await setDiscussionPub(_postId, false);
}

async function exit(_postId, _progress, _users) {

    switch (_progress) {

        case progress.none:
            break;

        case progress.standby:
            break;

        case progress.ready:
            break;

        case progress.discussion:
            await exitDiscussion(_postId, _progress, _users);
            break;

        case progress.vote:
            break;

        case progress.result:
            await exitResult(_postId, _progress, _users);
            break;

        default:
            console.log('exit', 'unknown progress ', _progress);
            break;
    }
}

async function entry(_postId, _progress, _users) {

    switch (_progress) {

        case progress.none:
            await entryNone(_postId, _progress, _users);
            break;

        case progress.standby:
            await entryStandby(_postId, _progress, _users);
            break;

        case progress.ready:
            await entryReady(_postId, _progress, _users);
            break;

        case progress.discussion:
            await entryDiscussion(_postId, _progress, _users);
            break;

        case progress.vote:
            await entryVote(_postId, _progress, _users);
            break;

        case progress.result:
            await entryResult(_postId, _progress, _users);
            break;

        default:
            console.log('entry', 'unknown progress ', _progress);
            break;
    }
}

async function getUserNotifyTable(_preProgress, _nextProgress, _preState, _nextState) {

    let data = null;
    let notify = 'none';

    data = userNotifyTable.find((v) => _preProgress === v.preProgress && _nextProgress === v.nextProgress &&
        _preState === v.preState && _nextState === v.nextState);

    if (undefined !== data) {
        notify = data.notify;
    }

    return notify;
}

async function getUserNotify(_postId, _preProgress, _nextProgress, _preState, _nextState, _user, _attendees) {

    let msg = null;
    let notify = null;
    let limitTime = null;

    // 通知を取得する
    notify = await getUserNotifyTable(_preProgress, _nextProgress, _preState, _nextState);

    switch (notify) {

        // 討論待機要求通知
        case userNorify.notifyStandbyRequest:
            msg = {
                notify,
                data: {
                    attendees: _attendees
                }
            };
            break;

        // 討論準備要求通知
        case userNorify.notifyReadyRequest:
            const config = await getDiscussionMeetingConfig(_postId, _user.socketId);
            if (null !== config) {
                msg = {
                    notify,
                    data: {
                        config,
                        attendees: _attendees
                    }
                };
            }
            break;

        // 討論開始要求通知
        case userNorify.notifyStartRequest:
            limitTime = await getDiscussionLimitTime(_postId);
            msg = {
                notify,
                data: {
                    limitTime,
                    attendees: _attendees
                }
            };
            break;

        // 討論参加不可通知
        case userNorify.notifyJoinImpossibleRequest:
            msg = {
                notify,
                data: {
                    attendees: _attendees
                }
            };
            break;

        // 討論結果投票要求通知
        case userNorify.notifyVoteRequest:
            limitTime = await getDiscussionLimitTime(_postId);
            msg = {
                notify,
                data: {
                    limitTime,
                    attendees: _attendees
                }
            };
            break;

        // 討論結果取得要求通知
        case userNorify.notifyResultRequest:
            const result = await getDiscussionResult(_postId);
            msg = {
                notify,
                data: {
                    result,
                    attendees: _attendees
                }
            };
            break;

        default:
            break;
    }

    return msg;
}

function getUsersImage(_image) {

    let users = [];
    const watchers = _image.watchers.L;

    // 肯定をユーザー配列に追加する
    users.push({
        type: userJoinType.positive,
        userId: _image.positive.M.userId.S,
        state: _image.positive.M.state.S,
        socketId: _image.positive.M.socketId.S,
        text: _image.positive.M.text.S,
    });

    // 否定をユーザー配列に追加する
    users.push({
        type: userJoinType.negative,
        userId: _image.negative.M.userId.S,
        state: _image.negative.M.state.S,
        socketId: _image.negative.M.socketId.S,
        text: _image.negative.M.text.S,
    });

    // 視聴者をユーザー配列に追加する
    for (let i = 0; i < watchers.length; i++) {
        users.push({
            type: userJoinType.watcher,
            userId: watchers[i].M.userId.S,
            state: watchers[i].M.state.S,
            socketId: watchers[i].M.socketId.S,
            judge: watchers[i].M.judge.S
        });
    }

    return {
        progress: _image.progress.S,
        limitTime: _image.limitTime.N,
        users: users
    };
}

async function writeProgressCache(_postId, _preProgress, _nextProgress) {

    await Put({
        TableName: 'progressCacheTable',
        Item: {
            postId: _postId,
            cachePreProgress: _preProgress,
            cacheNextProgress: _nextProgress
        }
    });

}

async function readProgressCache(_postId) {

    let cachePreProgress = null;
    let cacheNextProgress = null;

    const { data } = await Query({
        TableName: 'progressCacheTable',
        KeyConditionExpression: '#postId = :postId',
        ExpressionAttributeNames: {
            '#postId': 'postId',
        },
        ExpressionAttributeValues: {
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        cachePreProgress = data.Items[0].cachePreProgress;
        cacheNextProgress = data.Items[0].cacheNextProgress;
    }

    return {
        cachePreProgress,
        cacheNextProgress
    };
}

async function writeUserCache(_postId, _socketId, _preProgress, _nextProgress, _preUserState, _nextUserState) {

    await Put({
        TableName: 'userCacheTable',
        Item: {
            postId: _postId,
            socketId: _socketId,
            cachePreProgress: _preProgress,
            cacheNextProgress: _nextProgress,
            cachePreUserState: _preUserState,
            cacheNextUserState: _nextUserState
        }
    });

}

async function readUserCache(_postId, _socketId) {

    let cachePreProgress = null;
    let cacheNextProgress = null;
    let cachePreUserState = null;
    let cacheNextUserState = null;

    const { data } = await Query({
        TableName: 'userCacheTable',
        KeyConditionExpression: '#postId = :postId AND #socketId = :socketId',
        ExpressionAttributeNames: {
            '#postId': 'postId',
            '#socketId': 'socketId',
        },
        ExpressionAttributeValues: {
            ':postId': _postId,
            ':socketId': _socketId
        }
    });

    if (0 < data.Count) {
        cachePreProgress = data.Items[0].cachePreProgress;
        cacheNextProgress = data.Items[0].cacheNextProgress;
        cachePreUserState = data.Items[0].cachePreUserState;
        cacheNextUserState = data.Items[0].cacheNextUserState;
    }

    return {
        cachePreProgress,
        cacheNextProgress,
        cachePreUserState,
        cacheNextUserState
    };
}

function getAttendeesJson(_users) {

    const positive = _users.filter((v) => v.type === userJoinType.positive);
    const negative = _users.filter((v) => v.type === userJoinType.negative);
    const watcher = _users.filter((v) => v.type === userJoinType.watcher);
    const watchers = [];

    for (let index = 0; index < watcher.length; index++) {
        watchers.push({
            userId: watcher[index].userId
        });
    }

    const json = {
        positive: {
            userId: positive[0].userId,
            text: positive[0].text,
        },
        negative: {
            userId: negative[0].userId || 'none',
            text: negative[0].text,
        },
        watchers
    };

    return json;
}

function checkAttendees(_old, _latest) {

    const old_positive = _old.filter((v) => v.type === userJoinType.positive);
    const old_negative = _old.filter((v) => v.type === userJoinType.negative);
    const old_watchers = _old.filter((v) => v.type === userJoinType.watcher);
    const latest_positive = _latest.filter((v) => v.type === userJoinType.positive);
    const latest_negative = _latest.filter((v) => v.type === userJoinType.negative);
    const latest_watchers = _latest.filter((v) => v.type === userJoinType.watcher);

    if (old_positive[0].userId !== latest_positive[0].userId ||
        old_negative[0].userId !== latest_negative[0].userId) {
        return true;
    }
    if (old_watchers.length !== latest_watchers.length) {
        return true;
    }

    return false;
}

function stateHandling(records) {

    return new Promise(async (resolve) => {

        for (let i = 0; i < records.length; i++) {

            let promises = [];
            let results = [];

            const { postId, oldNextProgress, latestNextProgress, oldUserImage, latestUserImage } = records[i];

            // 進捗に変化がある場合
            if (oldNextProgress !== latestNextProgress) {

                // 進捗のキャッシュを取得する
                const { cachePreProgress, cacheNextProgress } = await readProgressCache(postId);

                // キャッシュと変化があれば状態遷移を行う
                if (latestUserImage.progress !== cachePreProgress ||
                    latestNextProgress !== cacheNextProgress) {

                    //console.log('progress', 'cache ', cachePreProgress + '->' + cacheNextProgress);
                    //console.log('progress', 'latest', latestUserImage.progress + '->' + latestNextProgress);

                    // 退出処理
                    await exit(postId, latestUserImage.progress, latestUserImage.users);

                    // 進捗状況を更新する
                    await setDiscussionProgress(postId, latestNextProgress);

                    // 進入処理
                    await entry(postId, latestNextProgress, latestUserImage.users);

                    // 進捗のキャッシュを設定する
                    await writeProgressCache(postId, latestUserImage.progress, latestNextProgress);
                }
            }

            // 状態に変化があるユーザーに通知を行う
            for (let i = 0; i < latestUserImage.users.length; i++) {

                promises.push(new Promise(async (resolve) => {

                    let message = null;
                    let preState = userState.none;

                    // attendeesのJsonデータを取得する
                    const attendees = getAttendeesJson(latestUserImage.users);

                    // ユーザーのOldImageの状態を取得する
                    const user = oldUserImage.users.find((v) => v.type === latestUserImage.users[i].type && v.socketId === latestUserImage.users[i].socketId);

                    // データがある場合は状態を設定する
                    if (undefined !== user) {
                        preState = user.state;
                    }

                    // キャッシュと変化があれば通知を行う
                    if (latestUserImage.progress !== latestNextProgress ||
                        preState !== latestUserImage.users[i].state) {

                        // ユーザー状態遷移のキャッシュを取得する
                        const { cachePreProgress, cacheNextProgress, cachePreUserState, cacheNextUserState } = await readUserCache(postId, latestUserImage.users[i].socketId);

                        // キャッシュと変化があれば通知を行う
                        if (latestUserImage.progress !== cachePreProgress ||
                            latestNextProgress !== cacheNextProgress ||
                            preState !== cachePreUserState ||
                            latestUserImage.users[i].state !== cacheNextUserState) {

                            //console.log('user', 'cache ', latestUserImage.users[i].type, cachePreProgress + '->' + cacheNextProgress, cachePreUserState + '->' + cacheNextUserState);
                            //console.log('user', 'latest', latestUserImage.users[i].type, latestUserImage.progress + '->' + latestNextProgress, preState + '->' + latestUserImage.users[i].state);

                            // 通知を取得する
                            message = await getUserNotify(postId, latestUserImage.progress, latestNextProgress, preState, latestUserImage.users[i].state, latestUserImage.users[i], attendees);

                            // 通知があれば追加する
                            if (null !== message) {
                                await notify(latestUserImage.users[i].socketId, message);
                            }

                            // ユーザー状態遷移のキャッシュを設定する
                            await writeUserCache(postId, latestUserImage.users[i].socketId, latestUserImage.progress, latestNextProgress, preState, latestUserImage.users[i].state);
                        }
                    }

                    if (null === message) {

                        // 参加者数に変化があれば討論状態変化通知を通知する
                        if (checkAttendees(oldUserImage.users, latestUserImage.users)) {
                            await notify(latestUserImage.users[i].socketId, {
                                notify: 'notifyDiscussionStatus',
                                data: {
                                    attendees
                                }
                            });
                        }
                    }

                    // return 
                    resolve({
                        socketId: latestUserImage.users[i].socketId,
                        type: latestUserImage.users[i].type,
                        preState,
                        state: latestUserImage.users[i].state,
                        notify: JSON.stringify(message)
                    });

                }));
            }

            // ユーザー通知実行
            results = await Promise.all(promises);

            console.log(postId, '[' + i + ']', latestUserImage.progress + '->' + latestNextProgress, results, JSON.stringify(records[i]));
        }

        // 終了
        resolve(true);
    });
}

async function dynamoDbTriggerHandler(records) {

    let promises = [];
    let discussions = {};

    for (let i = 0; i < records.length; i++) {

        console.log(JSON.stringify(records[i]));

        if ('MODIFY' === records[i].eventName) {

            const postId = Number(records[i].dynamodb.Keys.postId.N);
            const oldUserImage = getUsersImage(records[i].dynamodb.OldImage);
            const latestUserImage = getUsersImage(records[i].dynamodb.NewImage);
            const oldNextProgress = checkProgress(oldUserImage);
            const latestNextProgress = checkProgress(latestUserImage);

            if (undefined === discussions[postId]) {
                discussions[postId] = { records: [] };
            }

            discussions[postId].records.push({
                postId,
                oldUserImage,
                latestUserImage,
                oldNextProgress,
                latestNextProgress
            });
        }
    }

    for (let key in discussions) {
        promises.push(stateHandling(discussions[key].records));
    }

    await Promise.all(promises);
}

exports.handler = async (event) => {

    try {

        // ハンドラ処理
        await dynamoDbTriggerHandler(event.Records);

    } catch (e) {
        console.error('handler', e, event);
    }

    return {
        statusCode: 200,
        body: null,
    };
};
