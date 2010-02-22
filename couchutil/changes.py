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
        while True:
            changes_resource = self.db.resource('_changes')
            startkey = self._read_startkey()
            args = {'feed': 'longpoll'}
            if startkey is not None:
                args['since'] = startkey
            headers, changes = changes_resource.get(**args)
            # We could get receive a lot more changes than the batch_size here,
            # i.e. if we haven't watched the database for a while (or ever), so
            # we need to split into batch_size'd chunks to avoid consuming lots
            # of memory.
            for batch in ibatch(changes['results'], self.batch_size):
                # We need a list to a) avoid consuming the iterator, and b) to
                # get the 'seq' from the last item.
                batch = list(batch)
                self.handle_changes([result['id'] for result in batch])
                # We know we've handled all of the changes in this batch so
                # update the state.
                self._write_startkey(batch[-1]['seq'])
            # We got to the end of the _changes results. Update the state one
            # last time.
            self._write_startkey(changes['last_seq'])

    def run_once(self):
        while True:
            startkey = self._read_startkey()
            log.debug('Reading updates from %r', startkey)
            args = {'limit': self.batch_size}
            if startkey is not None:
                args['startkey'] = startkey
            rows = list(self.db.view('_all_docs_by_seq', **args))
            if not rows:
                break
            self.handle_changes([row['id'] for row in rows])
            self._write_startkey(rows[-1]['key'])

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

