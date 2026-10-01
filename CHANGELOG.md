# Changelog

## [0.1.0](https://github.com/osodevops/kafka-connect-servicenow-oss/compare/v0.0.1...v0.1.0) (2026-10-01)


### Features

* Confluent migration tooling and quickstart examples ([4c20b2f](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/4c20b2fe526a3f3b22eb79d764d1c6932c6f7a02))
* **core:** snow-core shared library and fake ServiceNow harness ([4f2d0b4](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/4f2d0b4bdf3c461f8e747d703957256ab03126e4))
* docker e2e tests, generated configuration reference and plugin ZIP checks ([4108877](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/41088772f8f7eb952802ca231f9f85169db090cb))
* JMX metrics for source tables and the sink writer ([8f47665](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/8f47665e7ef4be8e39d0d689d3b89bfa25aa47c7))
* **sink:** ServiceNow sink connector ([ae61a8b](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/ae61a8bd43802484be0ab74399fac2d6afa1f32f))
* **source:** ServiceNow source connector ([82c1f4c](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/82c1f4c2f515aa845fe5a277ca44cfb9ca9a8690))


### Bug Fixes

* **source:** build partition maps with HashMap ordering so stored offsets resolve after a restart ([0cf26b2](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/0cf26b27d395c6f347c712b9a04511b2884a5618))
* **tools:** drop servicenow.ssl.key.password instead of emitting an undefined key ([3523198](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/35231982a3238ad4f7e22daebd1871acb5f64721))


### Documentation

* documentation site content for getting started, concepts, reference and migration ([e09f91d](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/e09f91da1143679cd75c89ec14f0252d0c99afa3))
* record implementation deviations and tidy contributing guide ([d3fecb6](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/d3fecb6740a887d8c244635af4204ac3d57df7d2))


### Miscellaneous Chores

* prepare first public release ([1b3c361](https://github.com/osodevops/kafka-connect-servicenow-oss/commit/1b3c361d03422aae18d29d466d8446be2b1ae8c7))
