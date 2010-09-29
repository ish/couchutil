"""
General-purpose CouchDB utilities.
"""

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
        # Run once for efficiency, i.e. to avoid process changes one
        # at a time.
        self.run_once()
        # Start a continuous changes loop.
        startkey = self._read_startkey()
        options = {'feed': 'continuous', 'heartbeat': 15000}
        if startkey:
            options['since'] = startkey
        log.debug('Reading updates from %r', startkey)
        for o in _changes(self.db, **options):
            self.handle_changes([o['id']])
            self._write_startkey(o['seq'])

    def run_once(self):
        startkey = self._read_startkey()
        options = {'limit': self.batch_size}
        while True:
            log.debug('Reading updates from %r', startkey)
            if startkey:
                options['since'] = startkey
            o = _changes(self.db, **options)
            results = o['results']
            if not results:
                break
            self.handle_changes([r['id'] for r in results])
            startkey = results[-1]['seq']
            self._write_startkey(startkey)

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
        except (IOError, ValueError):
            return None

    def _write_startkey(self, startkey):
        open(self.__statefile, 'wb').write(str(startkey))


def _changes(db, **options):
    """
    Choose between API from couchdb-python or a more "quick and dirty"
    implementation.
    """
    if hasattr(db, 'changes'):
        return db.changes(**options)
    else:
        if options.get('feed') == 'continuous':
            return _changes_sim_continuous(db, **options)
        else:
            return _changes_sim(db, **options)


def _changes_sim(db, **options):
    changes_resource = db.resource('_changes')
    headers, changes = changes_resource.get(**options)
    return changes


def _changes_sim_continuous(db, **options):
    # couchdb-python < 0.7 doesn't have a changes API so simulate it
    # using a longpoll loop.
    options['feed'] = 'longpoll'
    while True:
        changes = _changes_sim(db, **options)
        results = changes['results']
        if results:
            for result in results:
                yield result
            options['since'] = result['seq']
        else:
            yield {'last_seq': changes['last_seq']}
            break

