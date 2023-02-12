const { Query, Update, Delete, TransWrite, Put } = require('./dynamoDb');
const { getUtcMsec } = require('./utils');
const { notify } = require('./apiGateway');
const { createMeeting, createAttendee, deleteMeeting } = require('./chime');
const { progress, userJoinType, userNorify } = require('./define');
const discussionWatcherMax = 100;

async function getDiscussion(_postId) {

    let discussion = null;

    const { data } = await Query({
        TableName: 'TABLE_DISCUSSION',
        KeyConditionExpression: '#postId = :postId',
        ExpressionAttributeNames: {
            '#postId': 'postId'
        },
        ExpressionAttributeValues: {
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        discussion = data.Items[0];
    }

    return discussion;
}

async function getWatcherIndex(_postId, _socketId, _userId) {

    let index = -1;
    const discussion = await getDiscussion(_postId);

    if (null !== discussion) {
        const { watchers } = discussion;
        const watcher = watchers.find((v) => v.socketId === _socketId && v.userId === _userId);
        index = watchers.indexOf(watcher);
    }

    return index;
}

async function getDiscussionMeeting(_postId) {

    let meeting = null;

    const { data } = await Query({
        TableName: 'TABLE_MEETING',
        KeyConditionExpression: '#postId = :postId',
        ProjectionExpression: '#Meeting',
        ExpressionAttributeNames: {
            '#postId': 'postId',
            '#Meeting': 'Meeting'
        },
        ExpressionAttributeValues: {
            ':postId': _postId
        }
    });

    if (0 < data.Count) {
        meeting = data.Items[0].Meeting;
    }

    return meeting;
}

async function getDiscussionAttendees(_postId) {

    let attendees = {
        positive: 0,
        negative: 0,
        watchers: 0
    };

    const { data } = await Query({
        TableName: 'TABLE_DISCUSSION',
        KeyConditionExpression: '#postId = :postId',
        ProjectionExpression: '#positive, #negative, #watchers',
        ExpressionAttributeNames: {
            '#postId': 'postId',
            '#positive': 'positive',
            '#negative': 'negative',
            '#watchers': 'watchers',
        },
        ExpressionAttributeValues: {
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
async function getDiscussions(_country, _keys) {

    let discussions = {
        Items: []
    };

    let param = {
        TableName: 'TABLE_DISCUSSION',
        IndexName: 'country-createAt-index',
        ScanIndexForward: false,
        KeyConditionExpression: '#country = :country',
        FilterExpression: '#pub = :pub',
        ExpressionAttributeNames: {
            '#country': 'country',
            '#pub': 'pub',
        },
        ExpressionAttributeValues: {
            ':pub': true,
            ':country': _country
        },
        Limit: 50
    };

    if (null !== _keys) {
        param.ExclusiveStartKey = {
            country: _country,
            createAt: _keys.createAt,
            postId: _keys.postId
        };
    }

    const res = await Query(param);

    if (res.result && 0 < res.data.Count) {
        discussions = res.data;
    }

    return discussions;
}

async function getDiscussionLimitTime(_postId) {

    let limitTime = 0;

    const { data } = await Query({
        TableName: 'TABLE_DISCUSSION',
        KeyConditionExpression: '#postId = :postId',
        ProjectionExpression: '#limitTime',
        ExpressionAttributeNames: {
            '#postId': 'postId',
            '#limitTime': 'limitTime'
        },
        ExpressionAttributeValues: {
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

async function getDiscussionMeetingConfig(_postId, _socketId) {

    let result = null;
    let meeting = null;
    let attendee = null;

    // ミーティング情報を取得する
    meeting = await getDiscussionMeeting(_postId);

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

async function getDiscussionResult(_postId) {

    let result = null;

    const { data } = await Query({
        TableName: 'TABLE_RESULT',
        KeyConditionExpression: '#postId = :postId',
        ProjectionExpression: '#positive, #negative, #win',
        ExpressionAttributeNames: {
            '#positive': 'positive',
            '#negative': 'negative',
            '#win': 'win',
            '#postId': 'postId',
        },
        ExpressionAttributeValues: {
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
        FilterExpression: '#postId = :postId',
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
        TableName: 'userTable',
        KeyConditionExpression: '#userId = :userId',
        ExpressionAttributeNames: {
            '#userId': 'userId'
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

async function setPositiveState(_postId, _socketId, _userId, _state) {

    return await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
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

async function reSetPositiveState(_postId, _socketId, _userId) {

    return await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
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

async function setNegativeState(_postId, _socketId, _userId, _state) {

    return await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
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

async function reSetNegativeState(_postId, _socketId, _userId) {

    await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
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

async function setWatcherState(_postId, _socketId, _userId, _state) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'set #watchers[' + index + '].#userId = :userId, #watchers[' + index + '].#socketId = :socketId, #watchers[' + index + '].#state = :state',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    '#state': 'state',
                    '#socketId': 'socketId',
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

async function reSetWatcherState(_postId, _socketId, _userId) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'remove #watchers[' + index + ']',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    '#socketId': 'socketId',
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

async function setWatcherVote(_postId, _socketId, _userId, _judge) {

    let index = 0;
    let res = null;

    while (true) {

        index = await getWatcherIndex(_postId, _socketId, _userId);

        if (-1 !== index) {

            res = await Update({
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'set #watchers[' + index + '].#judge = :judge',
                ConditionExpression: '#watchers[' + index + '].#socketId = :socketId AND #watchers[' + index + '].#userId = :userId',
                ExpressionAttributeNames: {
                    '#watchers': 'watchers',
                    '#judge': 'judge',
                    '#userId': 'userId',
                    '#socketId': 'socketId'
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

async function setDiscussion(_country, _postId, _userId, _title, _detail, _positiveText, _negativeText) {

    return await Put({
        TableName: 'TABLE_DISCUSSION',
        Item: {
            country: _country,
            postId: _postId,
            createAt: getUtcMsec(),
            pub: true,
            userId: _userId,
            title: _title,
            detail: _detail,
            progress: 'standby',
            limitTime: 0,
            positive: {
                text: _positiveText,
                userId: 'none',
                socketId: 'none',
                state: 'none',
                version: 0
            },
            negative: {
                text: _negativeText,
                userId: 'none',
                socketId: 'none',
                state: 'none',
                version: 0
            },
            watchers: []
        }
    });
}

async function setUser(_userId, _name) {

    return await Put({
        TableName: 'userTable',
        Item: {
            userId: _userId,
            createAt: getUtcMsec(),
            updateAt: getUtcMsec(),
            status: 'none',
            name: _name,
            result: {
                win: 0,
                lose: 0,
                draw: 0
            },
            version: 0
        }
    });
}

async function setUserResult(_userId, _type, _count) {

    switch (_type) {

        case userResultType.win:

            return await Update({
                TableName: 'userTable',
                Key: {
                    userId: _userId
                },
                UpdateExpression: 'set #result.#win = :win',
                ExpressionAttributeNames: {
                    '#result': 'result',
                    '#win': 'win'
                },
                ExpressionAttributeValues: {
                    ':win': _count
                }
            });

        case userResultType.lose:

            return await Update({
                TableName: 'userTable',
                Key: {
                    userId: _userId
                },
                UpdateExpression: 'set #result.#lose = :lose',
                ExpressionAttributeNames: {
                    '#result': 'result',
                    '#lose': 'lose'
                },
                ExpressionAttributeValues: {
                    ':lose': _count
                }
            });

        case userResultType.draw:

            return await Update({
                TableName: 'userTable',
                Key: {
                    userId: _userId
                },
                UpdateExpression: 'set #result.#draw = :draw',
                ExpressionAttributeNames: {
                    '#result': 'result',
                    '#draw': 'draw'
                },
                ExpressionAttributeValues: {
                    ':draw': _count
                }
            });

        default:
            break;
    }

}

async function setDiscussionLimitTime(_postId, _limitTime) {

    return await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
            postId: _postId
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

async function setDiscussionProgress(_postId, _progress) {

    await Update({
        TableName: 'TABLE_DISCUSSION',
        Key: {
            postId: _postId
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

async function setDiscussionMeeting(_postId) {

    const meeting = await createMeeting(_postId);

    await Put({
        TableName: 'TABLE_MEETING',
        Item: {
            postId: _postId,
            ...meeting
        }
    });
}

async function setDiscussionResult(_postId, _progress, _users) {

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
        TableName: 'TABLE_RESULT',
        Item: {
            postId: _postId,
            createAt: getUtcMsec(),
            ...data
        }
    });
}

async function joinDiscussionPositive(_postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    joinType: _joinType,
                    createAt: getUtcMsec()
                }
            }
        },
        {
            Update: {
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'set #positive.#state = :join, #positive.#socketId = :socketId, #positive.#userId = :userId',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND #positive.#state = :none AND #positive.#socketId = :none AND #positive.#userId = :none',
                ExpressionAttributeNames: {
                    '#progress': 'progress',
                    '#positive': 'positive',
                    '#userId': 'userId',
                    '#socketId': 'socketId',
                    '#state': 'state'
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

async function joinDiscussionNegative(_postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    joinType: _joinType,
                    createAt: getUtcMsec()
                }
            }
        },
        {
            Update: {
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'set #negative.#state = :join, #negative.#socketId = :socketId, #negative.#userId = :userId',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND #negative.#state = :none AND #negative.#socketId = :none AND #negative.#userId = :none',
                ExpressionAttributeNames: {
                    '#progress': 'progress',
                    '#negative': 'negative',
                    '#userId': 'userId',
                    '#socketId': 'socketId',
                    '#state': 'state'
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

async function joinDiscussionWatcher(_postId, _socketId, _userId, _joinType) {

    const { result } = await TransWrite([
        {
            Put: {
                TableName: 'socketTable',
                Item: {
                    type: 'user',
                    socketId: _socketId,
                    userId: _userId,
                    postId: _postId,
                    joinType: _joinType,
                    createAt: getUtcMsec()
                }
            }
        },
        {
            Update: {
                TableName: 'TABLE_DISCUSSION',
                Key: {
                    postId: _postId
                },
                UpdateExpression: 'SET #watchers = list_append(#watchers, :value)',
                ConditionExpression: '#progress <> :vote AND #progress <> :result AND size(#watchers) < :discussionWatcherMax',
                ExpressionAttributeNames: {
                    '#progress': 'progress',
                    '#watchers': 'watchers',
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

async function deleteDiscussionMeeting(_postId) {

    const { data } = await Query({
        TableName: 'TABLE_MEETING',
        KeyConditionExpression: '#postId = :postId',
        ProjectionExpression: '#Meeting',
        ExpressionAttributeNames: {
            '#postId': 'postId',
            '#Meeting': 'Meeting'
        },
        ExpressionAttributeValues: {
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
exports.setUserResult = setUserResult;
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
