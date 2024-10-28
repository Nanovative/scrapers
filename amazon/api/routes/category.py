import uuid
from fastapi import APIRouter
from shared.models.category import Category
from api.utils import get_category_pool, event_loop_lock

router = APIRouter()


@router.post("/replace")
async def replace_categories(categories: list[Category]):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()

    is_ok = await category_pool.replace(categories, None, event_loop_lock)

    response = {
        "request_id": request_id,
        "total": len(categories),
        "message": "ok" if is_ok else "failed",
    }

    return response


@router.get("/get_by_name")
async def get_category_by_name(name: str):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    category = await category_pool.get_by_name(name, None, event_loop_lock)

    response = {
        "request_id": request_id,
        "category": category if category else "not found",
    }

    return response


@router.get("/get_by_depth")
async def get_category_by_depth(depth: int, strict: bool = False):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    categories, num_of_categories = await category_pool.get_by_depth(
        depth, strict, None, event_loop_lock
    )

    response = {
        "request_id": request_id,
        "categories": categories,
        "count": num_of_categories,
    }

    return response


@router.get("/get_by_ancestor")
async def get_category_by_ancestor(ancestor: str):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    categories = await category_pool.get_by_ancestor(ancestor, None, event_loop_lock)

    response = {
        "request_id": request_id,
        "categories": categories,
    }

    return response


@router.get("/get_by_parent")
async def get_category_by_parent(parent: str):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    categories = await category_pool.get_by_parent(parent, None, event_loop_lock)

    response = {
        "request_id": request_id,
        "categories": categories,
    }

    return response


@router.get("/get_by_leaf")
async def get_category_by_leaf(is_leaf: bool):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    categories = await category_pool.get_by_leaf(is_leaf, None, event_loop_lock)

    response = {
        "request_id": request_id,
        "categories": categories,
    }

    return response


@router.post("/get_by_ancestors_and_depth")
async def get_category_by_ancestors_and_depth(ancestors: list[str], depth: int):
    request_id = uuid.uuid4()

    category_pool = await get_category_pool()
    categories = await category_pool.get_by_ancestors_and_depth(
        ancestors, depth, None, event_loop_lock
    )

    response = {
        "request_id": request_id,
        "categories": categories,
    }

    return response
