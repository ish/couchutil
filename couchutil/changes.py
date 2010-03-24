"""
General-purpose CouchDB utilities.
"""

import itertools
import logging
import time


log = logging.getLogger()


class ChangesProcessor(object):
    """
    Utility class to process updates in a CouchDB database since some known
    point in the database's history.

    When called, the processor uses the database's _all_docs_by_seq view to
    walk the changes in sequence. Progress is stored in the statefile to make
    future calls efficient.

    By default, the processor does nothing. Subclass it and override
    handle_update and handle_delete. You can also override at the
    handle_changes level if necessary.
    """

    def __init__(self, db, statefile, batch_size=25, forever=False, poll_delay=None):
        self.db = db
        self.__statefile = statefile
        self.batch_size = batch_size
        self.forever = forever
        self.poll_delay = poll_delay

    def __call__(self):
        if self.forever and self.poll_delay:
            self.run_forever_poll()
        elif self.forever:
            self.run_forever()
        else:
            self.run_once()

    def run_forever_poll(self):
        while True:
            self.run_once()
            time.sleep(self.poll_delay)

    def run_forever(self):
        # Released version of couchdb-python doesn't support feed='continuous'
        # so simulate it using a longpoll loop for now.
        changes_resource = self.db.resource('_changes')
        while True:
            startkey = self._read_startkey()
            args = {'feed': 'longpoll', 'limit': self.batch_size}
            if startkey is not None:
                args['since'] = startkey
            headers, changes = changes_resource.get(**args)
            # CouchDB 0.10 doesn't understand the limit arg we passed so batch
            # the results up to avoid doing too much in one go.
            for batch in ibatch(changes['results'], self.batch_size):
                batch = list(batch)
                self.handle_changes([result['id'] for result in batch])
                self._write_startkey(batch[-1]['seq'])

    def run_once(self):
        changes_resource = self.db.resource('_changes')
        while True:
            startkey = self._read_startkey()
            log.debug('Reading updates from %r', startkey)
            args = {'limit': self.batch_size}
            if startkey is not None:
                args['since'] = startkey
            headers, changes = changes_resource.get(**args)
            results = changes['results']
            if not results:
                break
            # CouchDB 0.10 doesn't understand the limit arg we passed so batch
            # the results up to avoid doing too much in one go.
            for batch in ibatch(changes['results'], self.batch_size):
                batch = list(batch)
                self.handle_changes([result['id'] for result in batch])
                self._write_startkey(batch[-1]['seq'])

    def handle_changes(self, ids):
        for row in self.db.view('_all_docs', keys=ids, include_docs=True):
            if row.doc is None:
                self.handle_delete(row.key)
            else:
                self.handle_update(row.doc)

    def handle_delete(self, docid):
        log.debug('Ignoring deletion: %s', docid)

    def handle_update(self, doc):
        log.debug('Ignoring update: %s@%s', doc['_id'], doc['_rev'])

    def _read_startkey(self):
        try:
            return int(open(self.__statefile, 'rb').read())
        except IOError:
            return None

    def _write_startkey(self, startkey):
        open(self.__statefile, 'wb').write(str(startkey))


def ibatch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = itertools.islice(sourceiter, size)
        yield itertools.chain([batchiter.next()], batchiter)

