==============
Scrapy project
==============

********************
Settings explanation
********************

- ``MONGO_URI`` - address of MongoDB (default 'mongodb://localhost:27017/');
- ``MONGO_DB`` - MongoDB database name (default 'houzz');
- ``MAX_COUNT`` - amount of profiles to extract;
- ``GEO_BIAS`` - in which country search coordinates first (default Japan);
- ``PROXY_ADDR`` - address of proxy

Spider has name ``profiles``. So, run the spider with the next command

.. code::

    scrapy crawl profiles --loglevel INFO


Spider that works with API has name ``api``. So, run it by

.. code::

    scrapy crawl api --loglevel INFO