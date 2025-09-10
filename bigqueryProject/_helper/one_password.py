import asyncio
import os
import json
from _helper.json import is_json
from onepassword.client import Client

# This function uses the One Password Service Account to decode the the JSON note and return an object.
async def get_json_note(vault_id, item_id):

    # Gets your service account token from the OP_SERVICE_ACCOUNT_TOKEN environment variable.
    token = os.environ.get('ONEPASSWORD_SERVICE_TOKEN', 'not set')

    print('1pass token')
    print(token)

    # Connects to 1Password. Fill in your own integration name and version.
    client = await Client.authenticate(auth=token, integration_name="My 1Password Integration", integration_version="v1.0.0")

    # Retrieves a secret from 1Password. Takes a secret reference as input and returns the secret to which it points.
    item = await client.items.get(vault_id, item_id)

    print('1pass item')
    print(item)

    response = None

    if is_json(item.notes):
        response = json.loads(item.notes)

    print("response = ")
    print(response)

    return response

def get_json_note_sync(vault_id, item_id):
    """Synchronous wrapper for the async function"""
    return asyncio.run(get_json_note(vault_id, item_id))
