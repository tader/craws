import aiobotocore
import asyncio
import curses
from botocore import xform_name
from typing import Any, Callable, Coroutine, Optional, List

from pytableview.view import TableView, IteratorDataSource


async def requires(loop, service: str, call: str) -> List[str]:
    session = aiobotocore.get_session(loop=loop)
    loader = session.get_component('data_loader')
    service_model = loader.load_service_model(service, 'service-2')
    required = service_model.get('shapes', {}).get(f'{call}Request', {}).get('required', [])
    return required


async def services(loop):
    session = aiobotocore.get_session(loop=loop)
    loader = session.get_component('data_loader')
    for service in loader.list_available_services('paginators-1'):
        yield {
            "Service": service,
            "Description": loader.load_service_model(service, 'service-2')\
                    ['metadata']\
                    ['serviceFullName'],
        }


async def paginators(loop, service):
    session = aiobotocore.get_session(loop=loop)
    loader = session.get_component('data_loader')
    for paginator in loader.load_service_model(service, 'paginators-1')['pagination'].keys():
        if len(await requires(loop, service, paginator)) == 0:
            yield {
                "Paginator": paginator,
            }


async def paginate(loop, service, paginator):
    session = aiobotocore.get_session(loop=loop)
    loader = session.get_component('data_loader')
    result_key = loader.load_service_model(service, 'paginators-1')['pagination'][paginator]['result_key']
    async with session.create_client(service) as client:
        async for page in client.get_paginator(xform_name(paginator)).paginate():
            for r in page[result_key]:
                yield r


async def select_from_iterator(loop, stdscr, iterator):
    ids = IteratorDataSource(iterator)
    tv = TableView(loop, stdscr, ids)
    return await tv.show()


async def run(loop, stdscr):
    service = (await select_from_iterator(loop, stdscr, services(loop)))
    if service:
        s = service['Service']
        paginator = (await select_from_iterator(loop, stdscr, paginators(loop, s)))
        if paginator:
            p = paginator['Paginator']
            return await select_from_iterator(loop, stdscr, paginate(loop, s, p))


async def async_main(loop):
    stdscr = curses.initscr()
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(32, 0, 7)  # head
    curses.init_pair(33, 0, 15)  # head odd
    curses.init_pair(34, 15, 0)  # cell
    curses.init_pair(35, 15, 8)  # cell odd
    curses.noecho()
    curses.cbreak()
    stdscr.keypad(True)

    try:
        result = await run(loop, stdscr)
    finally:
        curses.nocbreak()
        stdscr.keypad(False)
        curses.echo()
        curses.endwin()

    return result


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(async_main(loop))
    if result:
        print(f"Result: {result}")


if __name__ == "__main__":
    main()
