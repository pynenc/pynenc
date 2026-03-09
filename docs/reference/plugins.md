# Plugin Documentation

Pynenc uses a plugin architecture for production backends. Each plugin lives in its own repository and publishes its own documentation.

## Official Plugins

| Plugin   | Package           | Documentation                                                       | Repository                                                                     |
| -------- | ----------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Redis    | `pynenc-redis`    | [docs.pynenc.org/redis](https://pynenc-redis.readthedocs.io/)       | [github.com/pynenc/pynenc-redis](https://github.com/pynenc/pynenc-redis)       |
| MongoDB  | `pynenc-mongodb`  | [docs.pynenc.org/mongodb](https://pynenc-mongodb.readthedocs.io/)   | [github.com/pynenc/pynenc-mongodb](https://github.com/pynenc/pynenc-mongodb)   |
| RabbitMQ | `pynenc-rabbitmq` | [docs.pynenc.org/rabbitmq](https://pynenc-rabbitmq.readthedocs.io/) | [github.com/pynenc/pynenc-rabbitmq](https://github.com/pynenc/pynenc-rabbitmq) |

## How It Works

Plugins register themselves via Python entry points (`pynenc.plugins`). When installed, their builder methods, configuration classes, and components are automatically discovered by the core library.

```bash
pip install pynenc-redis
```

```python
from pynenc import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .redis(url="redis://localhost:6379")  # Provided by pynenc-redis plugin
    .process_runner()
    .build()
)
```

See {doc}`builder` for the full builder API reference.

## Plugin Documentation Strategy

Each plugin hosts its own Sphinx documentation in its repository and publishes to Read the Docs. The main pynenc docs link to plugin docs via [Sphinx intersphinx](https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html), enabling cross-references between the core library and plugin APIs.

### For plugin authors

To integrate your plugin's documentation with pynenc:

1. **Host Sphinx docs** in your plugin repository with their own `docs/conf.py` and `.readthedocs.yaml`
2. **Add intersphinx mapping** in your plugin's `conf.py` to cross-reference core pynenc docs:

   ```python
   intersphinx_mapping = {
       "python": ("https://docs.python.org/3", None),
       "pynenc": ("https://docs.pynenc.org/en/latest/", None),
   }
   ```

3. **Register on Read the Docs** so your plugin docs are built and hosted automatically
4. **Use MyST Markdown** for consistency with the core pynenc documentation style

### Cross-referencing from plugin docs

Reference core pynenc classes and functions from plugin documentation:

```markdown
See {external:py:class}`pynenc.app.Pynenc` for the main application class.
```

### Cross-referencing from core docs

Once a plugin is registered in the core `intersphinx_mapping`, its classes can be referenced:

```markdown
See {external:py:class}`pynenc_redis.broker.RedisBroker` for the Redis broker implementation.
```
