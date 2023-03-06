const {
    joinDiscussionPositive,
    deleteSocket,
    getSocket,
    reSetPositiveState,
    reSetNegativeState,
    reSetWatcherState,
    joinDiscussionNegative,
    joinDiscussionWatcher,
    setPositiveState,
    setNegativeState,
    setWatcherState,
    setWatcherVote
} = require('/opt/discussion');

exports.handler = async (event, context) => {

    try {

        //console.log('Received event:', JSON.stringify(event, null, 2));
        for (const record of event.Records) {
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('ascii');
            const message = JSON.parse(payload);
            console.log('message', message);
            switch (message.cmd) {

                case 'connect':
                    break;

                case 'disconnect':
                    {
                        const { connectionId } = message.data;
                        const socket = await getSocket('user', connectionId);
                        const { postId, socketId, userId, joinType } = socket;
                        switch (joinType) {
                            case 1:
                                await reSetPositiveState(postId, socketId, userId);
                                break;
                            case 2:
                                await reSetNegativeState(postId, socketId, userId);
                                break;
                            case 3:
                                await reSetWatcherState(postId, socketId, userId);
                                break;
                            default:
                                break;
                        }
                        await deleteSocket('user', socketId);
                    }
                    break;

                case 'joinDiscussionPositive':
                    {
                        const { postId, socketId, userId, joinType } = message.data;
                        await joinDiscussionPositive(postId, socketId, userId, joinType);
                    }
                    break;

                case 'joinDiscussionNegative':
                    {
                        const { postId, socketId, userId, joinType } = message.data;
                        await joinDiscussionNegative(postId, socketId, userId, joinType);
                    }
                    break;

                case 'joinDiscussionWatcher':
                    {
                        const { postId, socketId, userId, joinType } = message.data;
                        await joinDiscussionWatcher(postId, socketId, userId, joinType);
                    }
                    break;

                case 'setDiscussionPositive':
                    {
                        const { postId, state, socketId, userId } = message.data;
                        await setPositiveState(postId, socketId, userId, state);
                    }
                    break;

                case 'setDiscussionNegative':
                    {
                        const { postId, state, socketId, userId } = message.data;
                        await setNegativeState(postId, socketId, userId, state);
                    }
                    break;

                case 'setDiscussionWatcher':
                    {
                        const { postId, state, socketId, userId } = message.data;
                        await setWatcherState(postId, socketId, userId, state);
                    }
                    break;

                case 'setVote':
                    {
                        const { postId, socketId, userId, judge } = message.data;
                        await setWatcherVote(postId, socketId, userId, judge);
                    }
                    break;

                default:
                    console.log('unknown cmd', message.cmd);
                    break;
            }
        }
    } catch (e) {
        console.log('ERROR', e);
    }

    return `Successfully processed ${event.Records.length} records.`;
};
