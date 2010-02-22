from __future__ import with_statement

import contextlib
from datetime import datetime
import logging

import couchdb


log = logging.getLogger(__name__)


class ReservationError(Exception):
    """
    Base class for all reservation errors.
    """


class AlreadyExists(ReservationError):
    """
    Reservation already exists error.
    """


class ReservationManager(object):
    """
    CouchDB reservations manager.
    """

    def __init__(self, db, prefix=None):
        self.db = db
        self.prefix = prefix

    @contextlib.contextmanager
    def reservation(self, id):
        reservation_doc = self.reserve(id)
        try:
            yield
            del reservation_doc['lease']
            self.db.update([reservation_doc])
        except:
            try:
                # why doesn't couchdb-python return the _rev too??? we can't
                # delete it without that.
                self.db.delete(reservation_doc)
            except Exception, e:
                log.error(e)
            raise

    @contextlib.contextmanager
    def reservation_change(self, new_id, old_id):
        if old_id == new_id:
            yield
        else:
            with self.reservation(new_id):
                yield
                self.release(old_id)

    def reserve(self, id):
        try:
            docid = self.db.create(self._reservation_doc(id))
            return self.db.get(docid)
        except couchdb.ResourceConflict:
            raise AlreadyExists(id)

    def release(self, id):
        if self.prefix is not None:
            id = ''.join([self.prefix, id])
        doc = self.db.get(id)
        self.db.delete(doc)

    def reserved(self, **k):
        if 'startkey' in k:
            k['startkey'] = self._add_prefix(k['startkey'])
        if 'endkey' in k:
            k['endkey'] = self._add_prefix(k['endkey'])
        return (self._remove_prefix(row.key) for row in self.db.view('_all_docs', **k))

    def _reservation_doc(self, id):
        return {'_id': self._add_prefix(id), 'lease': datetime.utcnow().isoformat()}

    def _add_prefix(self, id):
        if self.prefix is None:
            return id
        return ''.join([self.prefix, id])

    def _remove_prefix(self, id):
        if self.prefix is None:
            return id
        return id[len(self.prefix):]


if __name__ == '__main__':

    import unittest
    import uuid

    class TestCase(unittest.TestCase):

        def setUp(self):
            self.db_name = 'test_reservation_'+uuid.uuid4().hex
            self.server = couchdb.Server()
            self.db = self.server.create(self.db_name)
            self.mgr = ReservationManager(self.db)

        def tearDown(self):
            self.server.delete(self.db_name)
            
        def test_reserve(self):
            with self.mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            assert self.db.get('u/foo') is not None
            assert self.db.get('d/foo') is not None

        def test_reserve_clash(self):
            try:
                with self.mgr.reservation('u/foo'):
                    with self.mgr.reservation('u/foo'):
                        docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            except AlreadyExists:
                pass
            assert self.db.get('d/foo') is None

        def test_reservation_failure(self):
            try:
                with self.mgr.reservation('u/foo'):
                    {}['foo']
            except KeyError:
                pass
            assert len(list(self.db.view('_all_docs'))) == 0

        def test_release(self):
            with self.mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            doc = self.db.get(docid)
            self.db.delete(doc)
            self.mgr.release('u/foo')
            assert len(list(self.db.view('_all_docs'))) == 0

        def test_lease_started(self):
            doc = self.mgr.reserve('u/foo')
            assert doc['lease']

        def test_lease_removed(self):
            with self.mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            doc = self.db.get('u/foo')
            assert 'lease' not in doc

        def test_reservation_change(self):
            with self.mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            with self.mgr.reservation_change('u/bar', 'u/foo'):
                doc = self.db.get(docid)
                doc['u'] = 'u/bar'
                self.db.update([doc])
            assert self.db.get('u/foo') is None
            assert self.db.get('u/bar') is not None
            assert self.db.get('d/foo')['u'] == 'u/bar'

        def test_reservation_change_same(self):
            with self.mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            with self.mgr.reservation_change('u/foo', 'u/foo'):
                doc = self.db.get(docid)
                doc['u'] = 'u/foo'
                self.db.update([doc])
            assert self.db.get('u/foo') is not None
            assert self.db.get('u/bar') is None
            assert self.db.get('d/foo')['u'] == 'u/foo'

        def test_prefix(self):
            mgr = ReservationManager(self.db, prefix='wibble/')
            with mgr.reservation('u/foo'):
                docid = self.db.create({'_id': 'd/foo', 'u': 'u/foo'})
            assert self.db.get('wibble/u/foo') is not None
            assert self.db.get('d/foo') is not None
            with mgr.reservation_change('u/bar', 'u/foo'):
                doc = self.db.get(docid)
                doc['u'] = 'u/foo'
                self.db.update([doc])
            assert self.db.get('wibble/u/foo') is None
            assert self.db.get('wibble/u/bar') is not None
            assert self.db.get('d/foo')['u'] == 'u/foo'

        def test_list_reserved(self):
            self._test_list_reserved(self.mgr)

        def test_list_reserved_prefix(self):
            self._test_list_reserved(ReservationManager(self.db, prefix='wibble/'))

        def _test_list_reserved(self, mgr):
            assert len(list(mgr.reserved(startkey='u/foo', endkey='u/foo'))) == 0
            mgr.reserve('u/foo')
            assert set(mgr.reserved()) == set(['u/foo'])
            mgr.reserve('u/bar')
            assert set(mgr.reserved()) == set(['u/foo', 'u/bar'])
            assert set(mgr.reserved(startkey='u/foo', endkey='u/foo')) == set(['u/foo'])
            assert set(mgr.reserved(startkey='u/bar')) == set(['u/foo', 'u/bar'])
            mgr.reserve('u/foo1')
            mgr.reserve('u/foo2')
            mgr.reserve('u/foo3')
            assert set(mgr.reserved(startkey='u/foo', endkey='u/foo\u9999')) == set(['u/foo', 'u/foo1', 'u/foo2', 'u/foo3'])


    unittest.main()

