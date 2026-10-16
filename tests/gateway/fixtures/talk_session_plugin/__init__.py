"""Frozen offline Talk consumer for the first host attachment contract.

Not the installed Talk plugin: this fixture owns no agent, database, provider or bot.
Its scripted PCM/transcript pairs prove host routing only, never real speech.
"""
SESSION = None
INVOCATION = None


class ScriptedTalkConsumer:
    def __init__(self, attachment):
        self.attachment = attachment
        self.scope = attachment.scope

    async def feed(self, pcm, transcript, *, input_id, timestamp, scope=None):
        assert type(pcm) is bytes and pcm
        return await self.attachment.submit(scope or self.scope, input_id=input_id,
                                            text=transcript, timestamp=timestamp)

    async def saved(self, admission_id):
        return await self.attachment.result(self.scope, admission_id)

    async def snapshot(self):
        return await self.attachment.snapshot(self.scope)


def register(ctx):
    def join(raw, invocation=None):
        global SESSION, INVOCATION
        if invocation is None:
            return 'host invocation required'
        INVOCATION = invocation
        SESSION = ScriptedTalkConsumer(invocation.capture_session_attachment())
        return 'offline attachment captured'

    ctx.register_command('talk-fixture', join, invocation_context=True)
    ctx.register_command('talk-legacy', lambda raw: 'legacy:' + raw)
