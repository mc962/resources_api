from datetime import datetime

from algoliasearch.exceptions import AlgoliaUnreachableHostException, AlgoliaException
from dateutil import parser
from flask import request, redirect
from prometheus_client import Counter, Summary
from sqlalchemy import or_, func
from sqlalchemy.exc import IntegrityError

import app.utils as utils
from app import Config, db, index
from app.api import bp
from app.api.auth import is_user_oc_member, authenticate
from app.api.validations import validate_resource, requires_body
from app.models import Language, Resource, Category, Key

# Metrics
failures_counter = Counter('my_failures', 'Number of exceptions raised')
latency_summary = Summary('request_latency_seconds', 'Length of request')

logger = utils.setup_logger('routes_logger')


# Routes
@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources', methods=['GET'], endpoint='get_resources')
def resources():
    return get_resources()


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources', methods=['POST'], endpoint='create_resource')
@requires_body
@authenticate
def post_resources():
    validation_errors = validate_resource(request)

    if validation_errors:
        return utils.standardize_response(payload=validation_errors, status_code=422)
    return create_resource(request.get_json(), db)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources/<int:id>', methods=['GET'], endpoint='get_resource')
def resource(resource_id):
    return get_resource(resource_id)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources/<int:id>', methods=['PUT'], endpoint='update_resource')
@requires_body
@authenticate
def put_resource(resource_id):
    validation_errors = validate_resource(request, resource_id)

    if validation_errors:
        return utils.standardize_response(payload=validation_errors, status_code=422)
    return update_resource(resource_id, request.get_json(), db)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources/<int:id>/upvote', methods=['PUT'])
def upvote(resource_id):
    return update_votes(resource_id, 'upvotes')


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources/<int:id>/downvote', methods=['PUT'])
def downvote(resource_id):
    return update_votes(resource_id, 'downvotes')


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/resources/<int:id>/click', methods=['PUT'])
def update_resource_click(resource_id):
    return add_click(resource_id)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/search', methods=['GET'])
def search():
    return search_results()


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/languages', methods=['GET'])
def languages():
    return get_languages()


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/languages/<int:id>', methods=['GET'], endpoint='get_language')
def language(resource_id):
    return get_language(resource_id)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/categories', methods=['GET'])
def categories():
    return get_categories()


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/categories/<int:id>', methods=['GET'], endpoint='get_category')
def category(resource_id):
    return get_category(resource_id)


@latency_summary.time()
@failures_counter.count_exceptions()
@bp.route('/apikey', methods=['POST'], endpoint='apikey')
@requires_body
def apikey():
    """
    Verify OC membership and return an API key. The API key will be
    saved in the DB to verify use as well as returned upon subsequent calls
    to this endpoint with the same OC credentials.
    """
    json = request.get_json()
    email = json.get('email')
    password = json.get('password')
    is_oc_member = is_user_oc_member(email, password)

    if not is_oc_member:
        message = "The email or password you submitted is incorrect"
        payload = {'errors': {"invalid-credentials": {"message": message}}}
        return utils.standardize_response(payload=payload, status_code=401)

    try:
        # We need to check the database for an existing key
        service_api_key = Key.query.filter_by(email=email).first()
        if not service_api_key:
            # Since they're already authenticated by is_oc_user(), we know we
            # can generate an API key for them if they don't already have one
            return utils.create_new_apikey(email, logger)
        logger.info(service_api_key.serialize)
        return utils.standardize_response(payload=dict(data=service_api_key.serialize))
    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)


# Helpers
def get_resource(resource_id):
    found_resource = Resource.query.get(resource_id)

    if found_resource:
        return utils.standardize_response(payload=dict(data=found_resource.serialize))

    return redirect('/404')


def get_resources():
    """
    Gets a paginated list of resources.

    If the URL parameters `languages` or `category` are found
    in the request, the list will be filtered by these parameters.

    The filters are case insensitive.
    """
    resource_paginator = utils.Paginator(Config.RESOURCE_PAGINATOR, request)

    # Fetch the filter params from the url, if they were provided.
    resources_languages = request.args.getlist('languages')
    resources_category = request.args.get('category')
    updated_after = request.args.get('updated_after')
    paid = request.args.get('paid')

    q = Resource.query

    # Filter on languages
    if resources_languages:
        # Take the list of languages they pass in, join them all with OR
        q = q.filter(
            or_(*map(Resource.languages.any,
                map(Language.name.ilike, resources_languages))
                )
            )

    # Filter on category
    if resources_category:
        q = q.filter(
            Resource.category.has(
                func.lower(Category.name) == resources_category.lower()
            )
        )

    # Filter on updated_after
    if updated_after:
        try:
            ua_date = parser.parse(updated_after)
            if ua_date > datetime.now():
                raise Exception("updated_after greater than today's date")
            ua_date = ua_date.strftime("%Y-%m-%d")
        except Exception as e:
            logger.exception(e)
            message = 'The value for "updated_after" is invalid'
            res = {"errors": {"unprocessable-entity": {"message": message}}}
            return utils.standardize_response(payload=res, status_code=422)

        q = q.filter(
            or_(
                Resource.created_at >= ua_date,
                Resource.last_updated >= ua_date
            )
        )

    # Filter on paid
    if isinstance(paid, str) and paid.lower() in ['true', 'false']:
        paid_as_bool = paid.lower() == 'true'
        q = q.filter(Resource.paid == paid_as_bool)

    try:
        paginated_resources = resource_paginator.paginated_data(q)
        if not paginated_resources:
            return redirect('/404')
        resource_list = [
            list_resource.serialize for list_resource in paginated_resources.items
        ]
        pagination_details = resource_paginator.pagination_details(paginated_resources)
    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)

    return utils.standardize_response(payload=dict(
        data=resource_list,
        **pagination_details))


def search_results():
    term = request.args.get('q', '', str)
    page = request.args.get('page', 0, int)
    page_size = request.args.get('page_size', Config.RESOURCE_PAGINATOR.per_page, int)

    # Fetch the filter params from the url, if they were provided.
    paid = request.args.get('paid')
    search_category = request.args.get('category')
    search_languages = request.args.getlist('languages')
    filters = []

    # Filter on paid
    if isinstance(paid, str):
        paid = paid.lower()
        # algolia filters boolean attributes with either 0 or 1
        if paid == 'true':
            filters.append('paid=1')
        elif paid == 'false':
            filters.append('paid=0')

    # Filter on category
    if isinstance(search_category, str):
        filters.append(
            f"category:{search_category}"
        )

    # Filter on languages
    if isinstance(search_languages, list):
        for i, _ in enumerate(search_languages):
            search_languages[i] = f"languages:{search_languages[i]}"

        # joining all possible language values to algolia filter query
        filters.append(f"( {' OR '.join(search_languages)} )")

    try:
        search_result = index.search(f'{term}', {
            'filters': "AND".join(filters),
            'page': page,
            'hitsPerPage': page_size
        })

    except (AlgoliaUnreachableHostException, AlgoliaException) as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)

    if page >= int(search_result['nbPages']):
        return redirect('/404')

    results = [utils.format_resource_search(result) for result in search_result['hits']]

    pagination_details = {
                "pagination_details": {
                    "page": search_result['page'],
                    "number_of_pages": search_result['nbPages'],
                    "records_per_page": search_result['hitsPerPage'],
                    "total_count": search_result['nbHits'],
                }
        }
    return utils.standardize_response(payload=dict(data=results, **pagination_details))


def get_languages():
    language_paginator = utils.Paginator(Config.LANGUAGE_PAGINATOR, request)
    query = Language.query

    try:
        paginated_languages = language_paginator.paginated_data(query)
        if not paginated_languages:
            return redirect('/404')
        language_list = [
            list_language.serialize for list_language in paginated_languages.items
        ]
        pagination_details = language_paginator.pagination_details(paginated_languages)
    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)

    return utils.standardize_response(payload=dict(
        data=language_list,
        **pagination_details))


def get_language(language_id):
    found_language = Language.query.get(language_id)

    if found_language:
        return utils.standardize_response(payload=dict(data=found_language.serialize))

    return redirect('/404')


def get_categories():
    try:
        category_paginator = utils.Paginator(Config.CATEGORY_PAGINATOR, request)
        query = Category.query

        paginated_categories = category_paginator.paginated_data(query)
        if not paginated_categories:
            return redirect('/404')
        category_list = [
            list_category.serialize for list_category in paginated_categories.items
        ]
        pagination_details = category_paginator.pagination_details(paginated_categories)
    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)

    return utils.standardize_response(payload=dict(
        data=category_list,
        **pagination_details))


def get_category(category_id):
    found_category = Category.query.get(category_id)

    if found_category:
        return utils.standardize_response(payload=dict(data=found_category.serialize))

    return redirect('/404')


def get_attributes(json):
    languages_list = Language.query.all()
    categories_list = Category.query.all()

    language_dict = {l.key(): l for l in languages_list}
    category_dict = {c.key(): c for c in categories_list}

    langs = []
    for lang in json.get('languages') or []:
        attrs_language = language_dict.get(lang)
        if not attrs_language:
            attrs_language = Language(name=lang)
        langs.append(attrs_language)
    categ = category_dict.get(json.get('category'), Category(name=json.get('category')))
    return langs, categ


def update_votes(votes_id, vote_direction):

    votes_resource = Resource.query.get(votes_id)

    if not votes_resource:
        return redirect('/404')

    initial_count = getattr(votes_resource, vote_direction)
    setattr(votes_resource, vote_direction, initial_count+1)
    db.session.commit()

    return utils.standardize_response(payload=dict(data=votes_resource.serialize))


def add_click(click_id):
    click_resource = Resource.query.get(click_id)

    if not click_resource:
        return redirect('/404')

    initial_count = getattr(click_resource, 'times_clicked')
    setattr(click_resource, 'times_clicked', initial_count + 1)
    db.session.commit()

    return utils.standardize_response(payload=dict(data=click_resource.serialize))


def update_resource(resource_id, json, db_instance):
    updateable_resource = Resource.query.get(resource_id)

    if not updateable_resource:
        return redirect('/404')

    langs, categ = get_attributes(json)
    index_object = {'objectID': resource_id}

    try:
        logger.info(f"Updating resource. Old data: {updateable_resource.serialize}")
        if json.get('languages'):
            updateable_resource.languages = langs
            index_object['languages'] = updateable_resource.serialize['languages']
        if json.get('category'):
            updateable_resource.category = categ
            index_object['category'] = categ.name
        if json.get('name'):
            updateable_resource.name = json.get('name')
            index_object['name'] = json.get('name')
        if json.get('url'):
            updateable_resource.url = json.get('url')
            index_object['url'] = json.get('url')
        if 'paid' in json:
            paid = json.get('paid')

            # Converts "false" and "true" to their bool
            if type(paid) is str and paid.lower() in ["true", "false"]:
                paid = paid.lower().strip() == "true"
            updateable_resource.paid = paid
            index_object['paid'] = paid
        if 'notes' in json:
            updateable_resource.notes = json.get('notes')
            index_object['notes'] = json.get('notes')

        db_instance.session.commit()

        try:
            index.partial_update_object(index_object)

        except (AlgoliaUnreachableHostException, AlgoliaException) as e:
            logger.exception(e)
            print(f"Algolia failed to update index for resource '{updateable_resource.name}'")

        return utils.standardize_response(
            payload=dict(data=updateable_resource.serialize)
            )

    except IntegrityError as e:
        logger.exception(e)
        return utils.standardize_response(status_code=422)

    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)


def create_resource(json, db_instance):
    langs, categ = get_attributes(json)
    # TODO I think this is not the right argument to pass. Resource as it is currently defined expects a TimestampMixin, as the first arg
    new_resource = Resource(
        name=json.get('name'),
        url=json.get('url'),
        category=categ,
        languages=langs,
        paid=json.get('paid'),
        notes=json.get('notes'))

    try:
        db_instance.session.add(new_resource)
        db_instance.session.commit()
        index.save_object(new_resource.serialize_algolia_search)

    except (AlgoliaUnreachableHostException, AlgoliaException) as e:
        logger.exception(e)
        print(f"Algolia failed to index new resource '{new_resource.name}'")

    except IntegrityError as e:
        logger.exception(e)
        return utils.standardize_response(status_code=422)

    except Exception as e:
        logger.exception(e)
        return utils.standardize_response(status_code=500)

    return utils.standardize_response(payload=dict(data=new_resource.serialize))
