import asyncio
import argparse
import logging
import aiofiles.os
import aiofiles as aiof
from pathlib import Path


async def read_folder(path: Path, folders: list):
    """
    Asynchronously reads the contents of the specified folder and its subfolders.

    Args:
        path (Path): The path to the folder to be read.
        folders (list): A list to store the folders found during the recursive traversal.

    Returns:
        None
    """
    for file in path.iterdir():
        if file.is_dir():
            folders.append(file)
            await read_folder(file, folders)


async def copy_file(src_file: Path, dest_file: Path):
    """
    Copy the contents of a file from the source path to the destination path asynchronously.

    Args:
        src_file (Path): The path to the source file.
        dest_file (Path): The path to the destination file.

    Returns:
        None
    """
    try:
        async with aiof.open(src_file, "rb") as src:
            content = await src.read()
        async with aiof.open(dest_file, "wb") as dst:
            await dst.write(content)
    except OSError as e:
        logging.error(e)


async def copy_files(path: Path, dest: Path):
    """
    Asynchronously copies files from the given path to the destination path.

    Parameters:
        path (Path): The source directory path to copy files from.
        dest (Path): The destination directory path to copy files to.
    """
    for file in path.iterdir():
        if file.is_file():
            folder = dest / file.suffix[1:]
            try:
                await aiof.os.makedirs(folder, exist_ok=True)
                await copy_file(file, folder / file.name)
            except OSError as e:
                logging.error(e)


async def main(source: Path, output: Path):
    """
    Asynchronously copies all files from the given source directory to the output directory.

    Args:
        source (Path): The source directory to copy files from.
        output (Path): The output directory to copy files to.

    Returns:
        None
    """
    logging_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main: before scanning and copying")
    logging.info(f"Source: {source}")
    logging.info(f"Output: {output}")

    folders = [source]
    try:
        await read_folder(source, folders)

        # copy files in each folder concurrently
        tasks = [copy_files(folder, output) for folder in folders]
        await asyncio.gather(*tasks)
    except OSError as e:
        logging.error(e)

    logging.info(f"All files copied to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy files asynchronously")
    parser.add_argument("--source", "-s", required=True, help="Source directory")
    parser.add_argument("--output", "-o", required=True, help="Output directory")
    args = vars(parser.parse_args())

    source = Path(args["source"])
    output = Path(args["output"])

    asyncio.run(main(source, output))
