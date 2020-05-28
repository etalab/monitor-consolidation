[![Update new data](https://github.com/etalab/monitor-consolidation/workflows/Update%20new%20data/badge.svg)](https://github.com/etalab/monitor-consolidation/actions?query=workflow%3A%22Update+new+data%22)

# Monitoring des consolidations
Calcule tous les jours des statistiques sur les jeux de données consolidés, à l'aide de [validata-core](https://pypi.org/project/validata-core/) et de [schema.data.gouv.fr](https://schema.data.gouv.fr).


## Jeux de données concernés

> ⚠️ schema.data.gouv.fr supporte [plusieurs formats de schémas](https://schema.data.gouv.fr/documentation/validation-schemas) (Table Schema, XSD, JSON Schema). Pour le moment, ce dépôt n'est capable d'évaluer la qualité d'un jeu de données consolidé que par rapport au standard Table Schema.

Actuellement, les jeux de données consolidés surveillés sont :

- [Infrastructure de recharge des véhicules électriques](https://www.data.gouv.fr/fr/datasets/fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/)
- [Base nationale des lieux de covoiturage](https://www.data.gouv.fr/fr/datasets/base-nationale-consolidee-des-lieux-de-covoiturage/)
- [Base nationale des lieux de stationnement](https://www.data.gouv.fr/fr/datasets/base-nationale-des-lieux-de-stationnement/)

Il est possible que cette liste ne soit pas exhaustive. En réalité, la qualité d'une consolidation est évaluée automatiquement par ce dépôt si :

- le schéma est au format Table Schema ;
- il existe un jeu de données consolidé par rapport à ce schéma sur data.gouv.fr ;
- les métadonnées du jeu de données consolidé [sont renseignées côté schema.data.gouv.fr](https://github.com/etalab/schema.data.gouv.fr/blob/master/aggregateur/static/consolidation.yml).
