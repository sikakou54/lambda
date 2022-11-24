const { Query, Update, Delete, TransWrite, Put } = require('./dynamoDb');
const { getTimeStamp } = require('./utils');
const { notify } = require('./apiGateway');
const { createMeeting, createAttendee, deleteMeeting } = require('./chime');
const { progress, userJoinType, userNorify } = require('./define');
const discussionWatcherMax = 100;

async function getDiscussion(_country, _postId) {

    let discussion = null;

    const { data } = await Query({
        TableName: "discussionTable",
        KeyConditionExpression: "#country = :country AND #postId = :postId",
        ExpressionAttributeNames: {
            '#country': 'country',
            "#postId": 'postId'
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        discussion = data.Items[0];
    }

    return discussion;
}

async function getWatcherIndex(_country, _postId, _socketId, _userId) {

    let index = -1;
    const discussion = await getDiscussion(_country, _postId);

    if (null !== discussion) {
        const { watchers } = discussion;
        const watcher = watchers.find((v) => v.socketId === _socketId && v.userId === _userId);
        index = watchers.indexOf(watcher);
    }

    return index;
}

async function getDiscussionMeeting(_country, _postId) {

    let meeting = null;

    const { data } = await Query({
        TableName: 'meetingTable',
        KeyConditionExpression: '#country = :country AND #postId = :postId',
        ProjectionExpression: '#Meeting',
        ExpressionAttributeNames: {
            '#country': 'country',
            '#postId': 'postId',
            '#Meeting': 'Meeting'
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        meeting = data.Items[0].Meeting;
    }

    return meeting;
}

async function getDiscussionAttendees(_country, _postId) {

    let attendees = {
        positive: 0,
        negative: 0,
        watchers: 0
    };

    const { data } = await Query({
        TableName: 'discussionTable',
        KeyConditionExpression: '#country = :country AND #postId = :postId',
        ProjectionExpression: '#positive, #negative, #watchers',
        ExpressionAttributeNames: {
            '#country': 'country',
            '#postId': 'postId',
            '#positive': 'positive',
            '#negative': 'negative',
            '#watchers': 'watchers',
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        attendees = {
            positive: data.Items[0].positive,
            negative: data.Items[0].negative,
            watchers: data.Items[0].watchers,
        };
    }

    return attendees;
}
async function getDiscussions(_country) {

    let discussions = {
        Items: []
    };

    // discussionTableのデータを取得する
    const res = await Query({
        TableName: "discussionTable",
        IndexName: "createAt-index",
        ScanIndexForward: false,
        KeyConditionExpression: "#country = :country",
        FilterExpression: "#pub = :pub",
        ExpressionAttributeNames: {
            '#country': 'country',
            '#pub': 'pub',
        },
        ExpressionAttributeValues: {
            ':pub': true,
            ':country': _country
        },
        Limit: 10
    });

    if (res.result && 0 < res.data.Count) {
        discussions = res.data;
    }

    return discussions;
}

async function getDiscussionLimitTime(_country, _postId) {

    let limitTime = 0;

    const { data } = await Query({
        TableName: 'discussionTable',
        KeyConditionExpression: '#country = :country AND #postId = :postId',
        ProjectionExpression: '#limitTime',
        ExpressionAttributeNames: {
            '#country': 'country',
            '#postId': 'postId',
            '#limitTime': 'limitTime'
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        limitTime = data.Items[0].limitTime;
    }

    return limitTime;
}

async function getDiscussionMeetingAttendees(_meetingId, _socketId) {

    return await createAttendee(_meetingId, _socketId);
}

async function getDiscussionMeetingConfig(_country, _postId, _socketId) {

    let result = null;
    let meeting = null;
    let attendee = null;

    // ミーティング情報を取得する
    meeting = await getDiscussionMeeting(_country, _postId);

    // 取得できた場合
    if (null !== meeting) {

        // 参加者情報を取得する
        attendee = await getDiscussionMeetingAttendees(meeting.MeetingId, _socketId);

        // 取得できた場合
        if (null !== attendee) {

            result = {
                Meeting: meeting,
                Attendee: attendee
            };
        }
    }

    return result;
}

async function getDiscussionResult(_country, _postId) {

    let result = null;

    const { data } = await Query({
        TableName: 'resultTable',
        KeyConditionExpression: '#country = :country AND #postId = :postId',
        ProjectionExpression: '#positive, #negative, #win',
        ExpressionAttributeNames: {
            '#positive': 'positive',
            '#negative': 'negative',
            '#win': 'win',
            '#country': 'country',
            '#postId': 'postId',
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        result = data.Items[0];
    }

    return result;
}

async function getSocket(_type, _socketId) {

    let socket = null;

    // discussionTableのデータを取得する
    const { data } = await Query({
        TableName: 'socketTable',
        KeyConditionExpression: '#type = :type AND #socketId = :socketId',
        ExpressionAttributeNames: {
            '#type': 'type',
            '#socketId': 'socketId'
        },
        ExpressionAttributeValues: {
            ':type': _type,
            ':socketId': _socketId
        }
    });

    if (0 < data.Count) {
        socket = data.Items[0];
    }

    return socket;
}

async function getSockets(_type, _postId) {

    let sockets = [];

    const { data } = await Query({
        TableName: 'socketTable',
        KeyConditionExpression: '#type = :type',
        FilterExpression: "#postId = :postId",
        ProjectionExpression: '#socketId',
        ExpressionAttributeNames: {
            '#type': 'type',
            '#postId': 'postId',
            '#socketId': 'socketId'
        },
        ExpressionAttributeValues: {
            ':type': _type,
            ':postId': _postId
        }
    });

    for (let i = 0; i < data.Items.length; i++) {
        sockets.push({
            socketId: data.Items[i].socketId
        });
    }

    return sockets;
}

async function getUser(_userId) {

    let user = null;

    // discussionTableのデータを取得する
    const { data } = await Query({
        TableName: "userTable",
        KeyConditionExpression: "#userId = :userId",
        ExpressionAttributeNames: {
            "#userId": 'userId'
        },
        ExpressionAttributeValues: {
            ':userId': _userId
        }
    });

    if (0 < data.Count) {
        user = data.Items[0];
    }

    return user;
}

async function setPositiveState(_country, _postId, _socketId, _userId, _state) {

    return await Update({
        TableName: 'discussionTable',
        Key: {
            country: _country,
            postId: _postId
        },
        UpdateExpression: 'SET #positive.#socketId = :socketId, #positive.#userId = :userId, #positive.#state = :state',
        ConditionExpression: '#positive.#socketId = :socketId AND #positive.#userId = :userId',
        ExpressionAttributeNames: {
            '#positive': 'positive',
            '#userId': 'userId',
            '#socketId': 'socketId',
            '#state': 'state'
        },
        ExpressionAttributeValues: {
            ':userId': _userId,
            ':socketId': _socketId,
            ':state': _state
        }
    });
}

async function reSetPositiveState(_country, _postId, _socketId, _userId) {

    return await Update({
        TableName: 'discussionTable',
        Key: {
            country: _country,
            postId: _postId
        },
        UpdateExpression: 'set #positive.#socketId = :none, #positive.#userId = :none, #positive.#state = :none',
        ConditionExpression: '#positive.#socketId = :socketId AND #positive.#userId = :userId',
        ExpressionAttributeNames: {
            '#positive': 'positive',
            '#userId': 'userId',
            '#socketId': 'socketId',
            '#state': 'state'
        },
        ExpressionAttributeValues: {
            ':none': 'none',
            ':socketId': _socketId,
            ':userId': _userId
        }
    });
}

async function setNegativeState(_country, _postId, _socketId, _userId, _state) {

    return await Update({
        TableName: 'discussionTable',
        Key: {
            country: _country,
            postId: _postId
        },
        UpdateExpression: 'SET #negative.#socketId = :socketId, #negative.#userId = :userId, #negative.#state = :state',
        ConditionExpression: '#negative.#socketId = :socketId AND #negative.#userId = :userId',
        ExpressionAttributeNames: {
            '#negative': 'negative',
            '#userId': 'userId',
            '#socketId': 'socketId',
            '#state': 'state'
        },
        ExpressionAttributeValues: {
            ':userId': _userId,
            ':socketId': _socketId,
            ':state': _state
        }
    });
}

async function reSetNegativeState(_country, _postId, _socketId, _userId) {

    await Update({
        TableName: 'discussionTable',
        Key: {
            country: _country,
            postId: _postId
        },
        UpdateExpression: 'set #negative.#socketId = :none, #negative.#userId = :none, #negative.#state = :none',
        ConditionExpression: '#negative.#socketId = :socketId AND #negative.#userId = :userId',
        ExpressionAttributeNames: {
            '#negative': 'negative',
            '#userId': 'userId',
            '#socketId': 'socketId',
            '#state': 'state'
        },
        ExpressionAttributeValues: {
            ':none': 'none',
            ':socketId': _socketId,
            ':userId': _userId
        }
    });
}

async function setWatcherState(_country, _postId, _socketId, _userId, _state) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_country, _postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'set #watchers[' + index + '].#userId = :userId, #watchers[' + index + '].#socketId = :socketId, #watchers[' + index + '].#state = :state',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    "#state": 'state',
                    "#socketId": 'socketId',
                    '#userId': 'userId'
                },
                ExpressionAttributeValues: {
                    ':state': _state,
                    ':userId': _userId,
                    ':socketId': _socketId
                }
            });

            if (!res.result) {
                if (400 === res.error.statusCode && 'ConditionalCheckFailedException' === res.error.code) {
                    continue;
                } else {
                    break;
                }
            } else {
                break;
            }

        } else {
            break;
        }
    }

    return res;
}

async function reSetWatcherState(_country, _postId, _socketId, _userId) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_country, _postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'remove #watchers[' + index + ']',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    "#socketId": 'socketId',
                    '#userId': 'userId'
                },
                ExpressionAttributeValues: {
                    ':userId': _userId,
                    ':socketId': _socketId
                }
            });

            if (!res.result) {
                if (!(400 === res.error.statusCode && 'ConditionalCheckFailedException' === res.error.code)) {
                    break;
                }
            } else {
                break;
            }

        } else {
            break;
        }
    }
}

async function setWatcherVote(_country, _postId, _socketId, _userId, _judge) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_country, _postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'set #watchers[' + index + '].#judge = :judge',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    "#judge": 'judge',
                    "#userId": 'userId',
                    "#socketId": 'socketId'
                },
                ExpressionAttributeValues: {
                    ':judge': _judge,
                    ':socketId': _socketId,
                    ':userId': _userId
                }
            });

            if (!res.result) {
                if (!(400 === res.error.statusCode && 'ConditionalCheckFailedException' === res.error.code)) {
                    break;
                }
            } else {
                break;
            }

        } else {
            break;
        }
    }
}

async function setDiscussion(_country, _postId, _userId, _title, _detail) {

    return await Put({
        TableName: "discussionTable",
        Item: {
            country: _country,
            postId: _postId,
            createAt: getTimeStamp(),
            pub: true,
            userId: _userId,
            title: _title,
            detail: _detail,
            progress: "standby",
            limitTime: 0,
            positive: {
                userId: 'none',
                socketId: "none",
                state: "none",
                version: 0
            },
            negative: {
                userId: 'none',
                socketId: "none",
                state: "none",
                version: 0
            },
            watchers: []
        }
    });
}

async function setUser(_userId, _name) {

    return await Put({
        TableName: "userTable",
        Item: {
            userId: _userId,
            createAt: getTimeStamp(),
            updateAt: getTimeStamp(),
            status: "none",
            name: _name,
            version: 0
        }
    });
}

async function setDiscussionLimitTime(_country, _postId, _limitTime) {

    return await Update({
        TableName: 'discussionTable',
        Key: {
            postId: _postId,
            country: _country
        },
        UpdateExpression: 'set #limitTime = :limitTime',
        ExpressionAttributeNames: {
            '#limitTime': 'limitTime'
        },
        ExpressionAttributeValues: {
            ':limitTime': _limitTime
        }
    });
}

async function setDiscussionProgress(_country, _postId, _progress) {

    await Update({
        TableName: 'discussionTable',
        Key: {
            postId: _postId,
            country: _country
        },
        UpdateExpression: 'set #progress = :progress',
        ExpressionAttributeNames: {
            '#progress': 'progress'
        },
        ExpressionAttributeValues: {
            ':progress': _progress
        }
    });
}

async function setDiscussionMeeting(_country, _postId) {

    const meeting = await createMeeting(_postId);

    await Put({
        TableName: 'meetingTable',
        Item: {
            country: _country,
            postId: _postId,
            ...meeting
        }
    });
}

async function setDiscussionResult(_country, _postId, _progress, _users) {

    let data = {};
    const positiveWatchers = _users.filter((v) => v.type === userJoinType.watcher && v.judge === userJoinType.positive);
    const negativeWatchers = _users.filter((v) => v.type === userJoinType.watcher && v.judge === userJoinType.negative);

    data.positive = positiveWatchers.length;
    data.negative = negativeWatchers.length;

    if (data.positive === data.negative) {
        data.win = 'draw';
    } else if (data.positive > data.negative) {
        data.win = userJoinType.positive;
    } else if (data.positive < data.negative) {
        data.win = userJoinType.negative;
    }

    await Put({
        TableName: 'resultTable',
        Item: {
            postId: _postId,
            country: _country,
            createAt: getTimeStamp(),
            ...data
        }
    });
}

async function joinDiscussionPositive(_country, _postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    country: _country,
                    joinType: _joinType,
                    createAt: getTimeStamp()
                }
            }
        },
        {
            Update: {
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'set #positive.#state = :join, #positive.#socketId = :socketId, #positive.#userId = :userId',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND #positive.#state = :none AND #positive.#socketId = :none AND #positive.#userId = :none',
                ExpressionAttributeNames: {
                    "#progress": 'progress',
                    '#positive': 'positive',
                    '#userId': 'userId',
                    "#socketId": 'socketId',
                    "#state": 'state'
                },
                ExpressionAttributeValues: {
                    ':userId': _userId,
                    ':socketId': _socketId,
                    ':join': 'join',
                    ':none': 'none',
                    ':vote': progress.vote,
                    ':result': progress.result
                }
            }
        }
    ]);

    if (!result) {
        await notify(_socketId, {
            notify: userNorify.notifyJoinImpossibleRequest,
            data: null
        });
    }
}

async function joinDiscussionNegative(_country, _postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    country: _country,
                    joinType: _joinType,
                    createAt: getTimeStamp()
                }
            }
        },
        {
            Update: {
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'set #negative.#state = :join, #negative.#socketId = :socketId, #negative.#userId = :userId',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND #negative.#state = :none AND #negative.#socketId = :none AND #negative.#userId = :none',
                ExpressionAttributeNames: {
                    "#progress": 'progress',
                    '#negative': 'negative',
                    '#userId': 'userId',
                    "#socketId": 'socketId',
                    "#state": 'state'
                },
                ExpressionAttributeValues: {
                    ':userId': _userId,
                    ':socketId': _socketId,
                    ':join': 'join',
                    ':none': 'none',
                    ':vote': progress.vote,
                    ':result': progress.result
                }
            }
        }
    ]);

    if (!result) {
        await notify(_socketId, {
            notify: userNorify.notifyJoinImpossibleRequest,
            data: null
        });
    }
}

async function joinDiscussionWatcher(_country, _postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    country: _country,
                    joinType: _joinType,
                    createAt: getTimeStamp()
                }
            }
        },
        {
            Update: {
                TableName: 'discussionTable',
                Key: {
                    postId: _postId,
                    country: _country
                },
                UpdateExpression: 'SET #watchers = list_append(#watchers, :value)',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND size(#watchers) < :discussionWatcherMax',
                ExpressionAttributeNames: {
                    "#progress": 'progress',
                    '#watchers': "watchers",
                },
                ExpressionAttributeValues: {
                    ':value': [{
                        userId: _userId,
                        socketId: _socketId,
                        state: 'join',
                        judge: 'none',
                        version: 0
                    }],
                    ':discussionWatcherMax': discussionWatcherMax,
                    ':vote': progress.vote,
                    ':result': progress.result
                }
            }
        }]);

    if (!result) {
        await notify(_socketId, {
            notify: userNorify.notifyJoinImpossibleRequest,
            data: null
        });
    }
}

async function deleteSocket(_type, _socketId) {

    return await Delete({
        TableName: 'socketTable',
        Key: {
            type: _type,
            socketId: _socketId
        }
    });
}

async function deleteDiscussionMeeting(_country, _postId) {

    const { data } = await Query({
        TableName: 'meetingTable',
        KeyConditionExpression: '#country = :country AND #postId = :postId',
        ProjectionExpression: '#Meeting',
        ExpressionAttributeNames: {
            '#country': 'country',
            '#postId': 'postId',
            '#Meeting': 'Meeting'
        },
        ExpressionAttributeValues: {
            ':country': _country,
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        await deleteMeeting(data.Items[0].Meeting.MeetingId);
    }
}

/**
 * exports
 */
exports.getDiscussionAttendees = getDiscussionAttendees;
exports.getDiscussions = getDiscussions;
exports.getDiscussion = getDiscussion;
exports.getWatcherIndex = getWatcherIndex;
exports.getSocket = getSocket;
exports.getSockets = getSockets;
exports.getDiscussionLimitTime = getDiscussionLimitTime;
exports.getDiscussionResult = getDiscussionResult;
exports.getDiscussionMeetingConfig = getDiscussionMeetingConfig;
exports.getUser = getUser;
exports.setPositiveState = setPositiveState;
exports.setNegativeState = setNegativeState;
exports.setWatcherVote = setWatcherVote;
exports.setUser = setUser;
exports.setWatcherState = setWatcherState;
exports.joinDiscussionPositive = joinDiscussionPositive;
exports.joinDiscussionNegative = joinDiscussionNegative;
exports.reSetWatcherState = reSetWatcherState;
exports.reSetNegativeState = reSetNegativeState;
exports.reSetPositiveState = reSetPositiveState;
exports.joinDiscussionWatcher = joinDiscussionWatcher;
exports.setDiscussion = setDiscussion;
exports.setDiscussionLimitTime = setDiscussionLimitTime;
exports.setDiscussionProgress = setDiscussionProgress;
exports.setDiscussionMeeting = setDiscussionMeeting;
exports.setDiscussionResult = setDiscussionResult;
exports.deleteSocket = deleteSocket;
exports.deleteDiscussionMeeting = deleteDiscussionMeeting;
