from blocktrail import connection
from datetime import datetime
import time
from blocktrail.exceptions import *
import requests
from requests.exceptions import ConnectionError
import logging


class APIClient(object):
    requests = 0
    max_requests_per_minute = 300

    def __init__(self, api_key, api_secret, network='BTC', testnet=False, api_version='v1', api_endpoint=None, debug=False):
        """
        :param str      api_key:        the API_KEY to use for authentication
        :param str      api_secret:     the API_SECRET to use for authentication
        :param str      network:        the crypto network to consume (eg BTC, LTC, etc)
        :param bool     testnet:        testnet network yes/no
        :param str      api_version:    the version of the API to consume
        :param str      api_endpoint:   overwrite the endpoint used
                                         this will cause the :network, :testnet and :api_version to be ignored!
        :param bool     debug:          print debug information when requests fail, and extra client info
        """
        self.init_time = datetime.now()
        if api_endpoint is None:
            network = ("t" if testnet else "") + network.upper()
            api_endpoint = "https://api.blocktrail.com/%s/%s" % (api_version, network)
        log_level = 'DEBUG' if debug else 'ERROR'
        logging.basicConfig(
            level = log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger('__main__')

        self.client = connection.RestClient(api_endpoint=api_endpoint, api_key=api_key, api_secret=api_secret, debug=debug)

    def _check_limit(self):
        print('self.requests', self.requests)
        now = datetime.now()
        td = now - self.init_time
        # print(td)
        if (now - self.init_time).seconds >= 60:
            self.requests = 0
            self.init_time = now
            return self.reset_limits()
        if self.requests >= 300:
            return 60 - (now - self.init_time).seconds

    def check_limit_and_sleep(self):
        sleep = self._check_limit()
        if sleep:
            print('Limit reached. Sleeping for %d seconds' % (sleep + 1))
            time.sleep(sleep + 1)
            self.reset_limits()
            return sleep
        return 0

    def reset_limits(self):
        print('Resetting requests to 0 and init_time to %s' % str(datetime.now()))
        self.requests = 0
        self.init_time = datetime.now()
        return 0

    def make_api_call(self, func, kwargs):

        try:
            self.check_limit_and_sleep()
            self.requests += 1
            return func(**kwargs)
        except RateLimitExceededError as e:
            print(e)
            print('About to sleep for 15 seconds')
            time.sleep(15)
            self.reset_limits()
            return self.make_api_call(func, kwargs)
        except (GenericHTTPError, GenericServerError) as e:
            print('Something went wrong their end. Sleep for a while and try again')
            print(e)
            time.sleep(15)
            return self.make_api_call(func, kwargs)
        except ConnectionError as e:
            print(e)
            print('Sleep for a while and try again')
            time.sleep(15)
            return self.make_api_call(func, kwargs)


    def address_response(self, address):
        """
        get a single address

        :param str      address:        the address hash
        :rtype: requests.Response
        """
        response = self.client.get("/address/%s" % (address, ))

        return response

    def address(self, address):
        return self.address_response(address).json()

    def address_transactions_response(self, address, page=1, limit=20, sort_dir='asc'):
        """
        get all transactions for an address (paginated)

        :param str      address:        the address hash
        :param int      page:           pagination page, starting at 1
        :param int      limit:          the amount of transactions per page, can be between 1 and 200
        :param str      address:        sorted ASC or DESC (on time)
        :rtype: requests.Response
        """

        response = self.client.get("/address/%s/transactions" % (address, ), params={'page': page, 'limit': limit, 'sort_dir': sort_dir})

        return response

    def address_transactions(self, address, page=1, limit=20, sort_dir='asc'):
        return self.address_transactions_response(address, page, limit, sort_dir).json()

    def address_unconfirmed_transactions_response(self, address, page=1, limit=20, sort_dir='asc'):
        """
        get all unconfirmed transactions for an address (paginated)

        :param str      address:        the address hash
        :param int      page:           pagination page, starting at 1
        :param int      limit:          the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:       sorted ASC or DESC (on time)
        :rtype: requests.Response
        """
        response = self.client.get("/address/%s/unconfirmed-transactions" % (address, ), params={'page': page, 'limit': limit, 'sort_dir': sort_dir})

        return response

    def address_unconfirmed_transactions(self, address, page=1, limit=20, sort_dir='asc'):
        return self.address_unconfirmed_transactions_response(address, page, limit, sort_dir).json()

    def address_unspent_outputs_response(self, address, page=1, limit=20, sort_dir='asc'):
        """
        get all inspent outputs for an address (paginated)

        :param str      address:        the address hash
        :param int      page:           pagination page, starting at 1
        :param int      limit:          the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:       sorted ASC or DESC (on time)
        :rtype: requests.Response
        """
        response = self.client.get("/address/%s/unspent-outputs" % (address, ), params={'page': page, 'limit': limit, 'sort_dir': sort_dir})

        return response

    def address_unspent_outputs(self, address, page=1, limit=20, sort_dir='asc'):
        return self.address_unspent_outputs_response(address, page, limit, sort_dir).json()

    def verify_address_response(self, address, signature):
        """
        verify ownership of an address

        :param str      address:        the address hash
        :param str      signature:      signature generated with PK with message being the :address
        :rtype: requests.Response
        """
        response = self.client.post("/address/%s/verify" % (address, ), data={'signature': signature}, auth=True)

        return response

    def verify_address(self, address, signature):
        return self.verify_address_response(address, signature).json()

    def all_blocks_response(self, page=1, limit=20, sort_dir='asc'):
        """
        get all blocks (paginated)

        :param int      page:            pagination page, starting at 1
        :param int      limit:           the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:        sorted ASC or DESC (on time)
        :rtype: requests.Response
        """

        response = self.client.get("/all-blocks", params={'page': page, 'limit': limit, 'sort_dir': sort_dir})

        return response

    def all_blocks(self, page=1, limit=20, sort_dir='asc'):
        """
        get all blocks (paginated)

        :param int      page:            pagination page, starting at 1
        :param int      limit:           the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:        sorted ASC or DESC (on time)
        :rtype: dict
        """

        return self.all_blocks_response(page, limit, sort_dir).json()

    def block_latest_response(self):
        """
        get the latest block

        :rtype: requests.Response
        """
        response = self.client.get("/block/latest")

        return response

    def block_latest(self):
        """
        get the latest block

        :rtype: dict
        """
        return self.block_latest_response().json()

    def block_response(self, block):
        """
        get a block

        :param str|int  block:           the block hash or block height
        :rtype: requests.Response
        """

        response = self.client.get("/block/%s" % (block, ))

        return response

    def block(self, block):
        return self.block_response(block).json()

    def block_transactions_response(self, block, page=1, limit=20, sort_dir='asc'):
        """
        get all transactions for a block (paginated)

        :param str|int  block:           the block hash or block height
        :param int      page:            pagination page, starting at 1
        :param int      limit:           the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:        sorted ASC or DESC (on time)
        :rtype: requests.Response
        """

        response = self.client.get("/block/%s/transactions" % (block, ), params={'page': page, 'limit': limit, 'sort_dir': sort_dir})

        return response

    def block_transactions(self, block, page=1, limit=20, sort_dir='asc'):
        """
        get all transactions for a block (paginated)

        :param str|int  block:           the block hash or block height
        :param int      page:            pagination page, starting at 1
        :param int      limit:           the amount of transactions per page, can be between 1 and 200
        :param str      sort_dir:        sorted ASC or DESC (on time)
        :rtype: dict
        """
        return self.block_transactions_response(block, page, limit, sort_dir).json()

    def transaction_response(self, txhash):
        """
        get a single transaction

        :param str      txhash:          the transaction hash
        :rtype: requests.Response
        """

        response = self.client.get("/transaction/%s" % (txhash, ))

        return response

    def transaction(self, txhash):
        """
        get a single transaction

        :param str      txhash:          the transaction hash
        :rtype: dict
        """
        return self.transaction_response(txhash).json()

    def all_webhooks(self, page=1, limit=20):
        """
        get all webhooks (paginated)

        :param int      page:            pagination page, starting at 1
        :param int      limit:           the amount of webhooks per page, can be between 1 and 200
        :rtype: dict
        """

        response = self.client.get("/webhooks", params={'page': page, 'limit': limit})

        return response.json()

    def webhook(self, identifier):
        """
        get a webhook by it's identifier

        :param str      identifier:      the webhook identifier
        :rtype: dict
        """

        response = self.client.get("/webhook/%s" % (identifier, ))

        return response.json()

    def setup_webhook(self, url, identifier=None):
        """
        create a new webhook

        :param str      url:            the url to receive the webhook events
        :param str      identifier:     a unique identifier to associate with this webhook (optional)
        :rtype: dict
        """
        response = self.client.post("/webhook", data={'url': url, 'identifier': identifier}, auth=True)

        return response.json()

    def update_webhook(self, identifier, new_url=None, new_identifier=None):
        """
        update an existing webhook

        :param str      identifier:     the webhook identifier
        :param str      new_url:        the new webhook url
        :param str      new_identifier: the new webhook identifier
        :rtype: dict
        """
        response = self.client.put("/webhook/%s" % (identifier, ),
                                   data={'url': new_url, 'identifier': new_identifier},
                                   auth=True)

        return response.json()

    def delete_webhook(self, identifier):
        """
        deletes an existing webhook and any event subscriptions associated with it

        :param str      identifier:     the webhook identifier
        :rtype: dict
        """
        response = self.client.delete("/webhook/%s" % (identifier, ), auth=True)

        return response.json()

    def webhook_events(self, identifier, page=1, limit=20):
        """
        get a paginated list of all the events a webhook is subscribed to

        :param str      identifier:     the webhook identifier
        :param int      page:           pagination page, starting at 1
        :param int      limit:          the amount of webhooks per page, can be between 1 and 200
        :rtype: dict
        """

        response = self.client.get("/webhook/%s/events" % (identifier, ), params={'page': page, 'limit': limit})

        return response.json()

    def subscribe_address_transactions(self, identifier, address, confirmations=6):
        """
        subscribes a webhook to transaction events on a particular address

        :param str      identifier:     the webhook identifier
        :param str      address:        the address hash
        :param str      confirmations:  the amount of confirmations to send
        :rtype: dict
        """
        response = self.client.post(
            "/webhook/%s/events" % (identifier, ),
            data={
                'event_type': 'address-transactions',
                'address': address,
                'confirmations': confirmations
            },
            auth=True
        )

        return response.json()

    def batch_subscribe_address_transactions(self, identifier, batch_data):
        """
        batch subscribes a webhook to multiple transaction events

        :param str      identifier:     the webhook identifier
        :param list     batch_data:
        :rtype: dict
        """
        for record in batch_data:
            record['event_type'] = 'address-transactions'

        response = self.client.post("/webhook/%s/events/batch" % (identifier, ), data=batch_data, auth=True)

        return response.json()

    def subscribe_new_blocks(self, identifier):
        """
        subscribes a webhook to new blocks

        :param str      identifier:     the webhook identifier
        :rtype: dict
        """
        response = self.client.post(
            "/webhook/%s/events" % (identifier, ),
            data={
                'event_type': 'block'
            },
            auth=True
        )

        return response.json()

    def subscribe_transaction(self, identifier, transaction, confirmations=6):
        """
        subscribes a webhook to events on a particular transaction

        :param str      identifier:     the webhook identifier
        :param str      transaction:    the transaction hash
        :param str      confirmations:  the amount of confirmations to send
        :rtype: dict
        """
        response = self.client.post(
            "/webhook/%s/events" % (identifier, ),
            data={
                'event_type': 'transaction',
                'transaction': transaction,
                'confirmations': confirmations
            },
            auth=True
        )

        return response.json()

    def unsubscribe_address_transactions(self, identifier, address):
        """
        unsubscribes a webhook to transaction events from a particular address

        :param str      identifier:     the webhook identifier
        :param str      address:        the address hash
        :rtype: dict
        """
        response = self.client.delete("/webhook/%s/address-transactions/%s" % (identifier, address), auth=True)

        return response.json()

    def unsubscribe_new_blocks(self, identifier):
        """
        unsubscribes a webhook from new blocks

        :param str      identifier:     the webhook identifier
        :rtype: dict
        """
        response = self.client.delete("/webhook/%s/block" % (identifier, ), auth=True)

        return response.json()

    def unsubscribe_transaction(self, identifier, transaction):
        """
        unsubscribes a webhook to to events on a particular transaction

        :param str      identifier:     the webhook identifier
        :param str      transaction:        the address hash
        :rtype: dict
        """
        response = self.client.delete("/webhook/%s/transaction/%s" % (identifier, transaction), auth=True)

        return response.json()

    def price(self):
        """
        get the current price index

        :rtype: dict
        """

        response = self.client.get("/price")

        return response.json()

    def verify_message(self, message, address, signature):
        """
        verify message signed bitcoin-core style

        :param str      message:
        :param str      address:
        :param str      signature:
        :rtype: dict
        """

        response = self.client.post("/verify_message", dict(
            message=message,
            address=address,
            signature=signature
        ))

        return response.json()['result']
