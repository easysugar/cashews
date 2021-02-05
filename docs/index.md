# Cachews

**Cachews** is async cache utils with simple api to build fast and reliable applications

**The key features are:**

* Decorator base API aka `@cachews(ttl=timdelta(minutes=10))`
* Multi backend ([Memory](#memory), [Redis](#redis)) with prefix base setup
* Cache invalidation by time, 'ttl' is a required parameter to avoid storage overflow and endless cache
* Can cache any objects securely (use [hash key](#redis)). 
* Cache invalidation auto system and API 
* Cache usage detection API
🔥 Client Side cache with redis


## Installation

<div class="termy">

```console
$ pip install cachews

---> 100%
```

</div>


