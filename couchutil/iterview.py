import itertools


def iterview(db, name, batch, wrapper=None, **options):
    """Iterate the rows in a view, fetching rows in batches and yielding one
    row at a time.

    Since the view's rows are fetched in batches any rows emitted for documents
    added, changed or deleted between requests may be missed or repeated.

    :param name: the name of the view; for custom views, use the format
                 ``design_docid/viewname``, that is, the document ID of the
                 design document and the name of the view, separated by a
                 slash.
    :param batch: number of rows to fetch per HTTP request.
    :param wrapper: an optional callable that should be used to wrap the result
                    rows
    :param options: optional query string parameters
    :return: row generator
    """

    # Check sane batch size.
    if batch <= 0:
        raise ValueError('batch must be 1 or more')

    # Save caller's limit, it must be handled manually.
    limit = options.get('limit')
    if limit is not None and limit <= 0:
        raise ValueError('limit must be 1 or more')

    while True:
        loop_limit = min(limit or batch, batch)

        # Get rows in batches, with one extra for start of next batch.
        options['limit'] = loop_limit+1
        rows = list(db.view(name, wrapper, **options))

        # Yield rows from this batch.
        for row in itertools.islice(rows, loop_limit):
            yield row

        # Decrement limit counter.
        if limit is not None:
            limit -= min(len(rows), batch)

        # Check if there is nothing else to yield.
        if len(rows) <= batch or (limit is not None and limit == 0):
            break

        # Update options with start keys for next loop.
        options.update(startkey=rows[-1]['key'], startkey_docid=rows[-1]['id'])
