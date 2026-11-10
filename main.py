import ast
import asyncio
import contextlib
import io
import os
import textwrap
import time
import traceback
import discord
from discord.ext import commands
from dotenv import load_dotenv

import pymongo
import bson
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from bson import json_util, ObjectId

load_dotenv()

bot = commands.Bot(
    command_prefix="j!",
    help_command=None,
    intents=discord.Intents.all()
)

MONGO_URI = os.getenv("MONGO_URI")
DEFAULT_DB_NAME = os.getenv("MONGO_DB", "database")

if not MONGO_URI:
    print("WARNING: MONGO_URI is not set in environment or .env file.")

mongo_client = pymongo.MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=5000,
    socketTimeoutMS=5000,
)

current_db_name = DEFAULT_DB_NAME

class SmartDB:
    """
    A smart proxy for MongoDB that enables querying collections across
    all databases seamlessly without being locked into a single database.
    """
    def __init__(self, client, active_db_name=DEFAULT_DB_NAME):
        self._client = client
        self._active_db_name = active_db_name
        self._col_cache = None
        self._last_refresh = 0

    def refresh_cache(self):
        mapping = {}
        try:
            for db_name in self._client.list_database_names():
                if db_name in ("admin", "config", "local"):
                    continue
                for col_name in self._client[db_name].list_collection_names():
                    if col_name not in mapping:
                        mapping[col_name] = []
                    mapping[col_name].append(self._client[db_name][col_name])
            self._col_cache = mapping
            self._last_refresh = time.time()
        except Exception:
            pass
        return self._col_cache or {}

    def get_collection_map(self):
        if self._col_cache is None or (time.time() - self._last_refresh > 30):
            return self.refresh_cache()
        return self._col_cache

    def __getattr__(self, name):
        try:
            if name in self._client.list_database_names():
                return self._client[name]
        except Exception:
            pass

        try:
            active_db = self._client[self._active_db_name]
            if name in active_db.list_collection_names():
                return active_db[name]
        except Exception:
            pass

        cmap = self.get_collection_map()
        if name in cmap:
            return cmap[name][0]

        return self._client[self._active_db_name][name]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __repr__(self):
        return f"<SmartDB active='{self._active_db_name}'>"

def get_current_db():
    return SmartDB(mongo_client, active_db_name=current_db_name)

def build_execution_env(ctx: commands.Context) -> dict:
    smart_db = SmartDB(mongo_client, active_db_name=current_db_name)
    cmap = smart_db.get_collection_map()

    env = {
        "client": mongo_client,
        "mongo_client": mongo_client,
        "db": smart_db,
        "database": mongo_client[current_db_name],
        "dbs": {},
        "pymongo": pymongo,
        "bson": bson,
        "ObjectId": ObjectId,
        "bot": bot,
        "ctx": ctx,
        "discord": discord,
        "asyncio": asyncio,
        "os": os,
        "time": time,
    }

    try:
        for db_name in mongo_client.list_database_names():
            if db_name not in ("admin", "config", "local"):
                db_obj = mongo_client[db_name]
                env[db_name] = db_obj
                env["dbs"][db_name] = db_obj
    except Exception:
        pass

    for col_name, cols in cmap.items():
        if col_name not in env:
            matched = [c for c in cols if c.database.name == current_db_name]
            env[col_name] = matched[0] if matched else cols[0]

    return env

def format_size(size_bytes: int) -> str:
    """Formats bytes into human readable KB, MB, GB string."""
    if size_bytes is None:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:3.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

def clean_code(content: str) -> str:
    """Strips markdown code blocks and excess whitespace."""
    content = content.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if len(lines) >= 2 and lines[-1].startswith("```"):
            content = "\n".join(lines[1:-1])
        else:
            content = content.strip("`")
    return content.strip("` \n")

async def execute_pymongo_code(code_str: str, env: dict):
    """Executes arbitrary Python/PyMongo code in the given environment and captures output."""
    code_str = clean_code(code_str)
    stdout = io.StringIO()

    try:
        parsed = ast.parse(code_str)
        if parsed.body and isinstance(parsed.body[-1], ast.Expr):
            parsed.body[-1] = ast.Return(value=parsed.body[-1].value)
            ast.fix_missing_locations(parsed)

        func_def = ast.AsyncFunctionDef(
            name="_eval_func",
            args=ast.arguments(
                posonlyargs=[],
                args=[],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[]
            ),
            body=parsed.body,
            decorator_list=[]
        )
        module = ast.Module(body=[func_def], type_ignores=[])
        ast.fix_missing_locations(module)

        compiled = compile(module, "<db-eval>", "exec")
        local_ns = {}
        exec(compiled, env, local_ns)
        func = local_ns["_eval_func"]

        with contextlib.redirect_stdout(stdout):
            result = await func()

    except SyntaxError:
        wrapped = "async def _eval_func():\n" + textwrap.indent(code_str, "    ")
        local_ns = {}
        exec(wrapped, env, local_ns)
        func = local_ns["_eval_func"]
        with contextlib.redirect_stdout(stdout):
            result = await func()

    if isinstance(result, (Cursor, CommandCursor)):
        docs = []
        for i, doc in enumerate(result):
            if i >= 50:
                break
            docs.append(doc)
        result = docs

    return result, stdout.getvalue()

async def setup_hook():
    try:
        await bot.load_extension("jishaku")
        print("Loaded extension: jishaku")
    except Exception as e:
        print(f"Failed to load jishaku extension: {e}")

bot.setup_hook = setup_hook

@bot.event
async def on_ready():
    print(f"Logged in as: {bot.user.name} (ID: {bot.user.id})")
    try:
        mongo_client.admin.command("ping")
        all_dbs = [d for d in mongo_client.list_database_names() if d not in ("admin", "config", "local")]
        print(f"MongoDB connected successfully. Available databases ({len(all_dbs)}): {all_dbs}")
    except Exception as e:
        print(f"MongoDB connection check failed: {e}")

@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.NotOwner):
        return await ctx.reply("⛔ This command is restricted to the bot owner.")
    if isinstance(error, commands.CommandNotFound):
        return
    print(f"Unhandled command error in {ctx.command}: {error}")

@bot.command(name="ping")
async def ping(ctx: commands.Context):
    """Checks bot and MongoDB latency."""
    discord_lat = round(bot.latency * 1000, 2)
    mongo_lat = None
    try:
        t0 = time.perf_counter()
        mongo_client.admin.command("ping")
        mongo_lat = round((time.perf_counter() - t0) * 1000, 2)
    except Exception:
        pass

    msg = f"🏓 **Pong!**\n- Discord Gateway: `{discord_lat}ms`"
    if mongo_lat is not None:
        msg += f"\n- MongoDB Latency: `{mongo_lat}ms`"
    else:
        msg += "\n- MongoDB: `Disconnected / Error`"
    await ctx.reply(msg)

@bot.command(name="db")
@commands.is_owner()
async def db_handler(ctx: commands.Context, *, command: str = None):
    """
    Handles MongoDB operations across all databases and collections using PyMongo.
    Supports subcommands (ping, dbs, use, collections, stats) and arbitrary PyMongo code execution.
    """
    global current_db_name

    if not command or command.strip().lower() == "help":
        embed = discord.Embed(
            title="🍃 MongoDB Cluster Manager (PyMongo)",
            color=discord.Color.green(),
            description="Manage and query **all databases and collections** across your MongoDB cluster."
        )

        try:
            t0 = time.perf_counter()
            mongo_client.admin.command("ping")
            lat = round((time.perf_counter() - t0) * 1000, 2)
            embed.add_field(name="📶 Status", value=f"🟢 Connected (`{lat}ms`)", inline=True)
        except Exception:
            embed.add_field(name="📶 Status", value="🔴 Connection Failed", inline=True)

        embed.add_field(name="📌 Active Database", value=f"`{current_db_name}`", inline=True)

        try:
            user_dbs = [d for d in mongo_client.list_database_names() if d not in ("admin", "config", "local")]
            embed.add_field(name="📚 Databases", value=f"`{len(user_dbs)}` available", inline=True)
        except Exception:
            pass

        embed.add_field(
            name="⚡ Quick Shortcuts",
            value=(
                "• `j!db cols` - List all collections across all databases\n"
                "• `j!db cols <dbname>` - List collections with doc counts for `<dbname>`\n"
                "• `j!db dbs` - List all databases and disk sizes\n"
                "• `j!db stats [db/col]` - View cluster, database, or collection statistics\n"
                "• `j!db use <dbname>` - Switch active default database\n"
                "• `j!db ping` - Test Mongo ping latency & server version"
            ),
            inline=False
        )
        embed.add_field(
            name="🐍 PyMongo Code Execution (All Collections Accessible)",
            value=(
                "All databases & collections are directly injected into scope!\n"
                "**Examples:**\n"
                "• `j!db db.tickets.find()` _(finds collection across any db)_\n"
                "• `j!db bitbotdb.tickets.find_one()`\n"
                "• `j!db wallet_db.wallet.count_documents({})`\n"
                "• `j!db tickets.find_one({'user_id': 123})`\n"
                "• `j!db toahalloweenbot.users.find()`\n"
                "• `j!db web_db.accounts.find()`\n"
                "• Multiline code blocks with \\`\\`\\`py ... \\`\\`\\`"
            ),
            inline=False
        )
        embed.set_footer(text=f"Prefix: {ctx.prefix} | PyMongo v{pymongo.__version__}")
        return await ctx.reply(embed=embed)

    raw_command = command.strip()
    cmd_lower = raw_command.lower()
    parts = raw_command.split()

    if cmd_lower == "ping":
        try:
            t0 = time.perf_counter()
            mongo_client.admin.command("ping")
            lat = round((time.perf_counter() - t0) * 1000, 2)
            info = mongo_client.server_info()
            version = info.get("version", "Unknown")

            embed = discord.Embed(
                title="🍃 MongoDB Ping",
                color=discord.Color.green(),
                description=f"✅ Successfully pinged MongoDB in **{lat}ms**"
            )
            embed.add_field(name="MongoDB Version", value=f"`{version}`", inline=True)
            embed.add_field(name="Active Database", value=f"`{current_db_name}`", inline=True)
            return await ctx.reply(embed=embed)
        except Exception as e:
            return await ctx.reply(f"❌ **MongoDB Ping Failed:**\n```\n{e}\n```")

    if cmd_lower in ("dbs", "list", "databases"):
        try:
            db_res = mongo_client.admin.command("listDatabases")
            databases = db_res.get("databases", [])
            total_size = format_size(db_res.get("totalSize", 0))

            lines = []
            for d in databases:
                name = d.get("name", "unknown")
                size = format_size(d.get("sizeOnDisk", 0))
                marker = "👉 " if name == current_db_name else "• "
                lines.append(f"{marker}**{name}** ({size})")

            embed = discord.Embed(
                title="📚 MongoDB Databases",
                description="\n".join(lines) if lines else "No databases found.",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Total Cluster Size: {total_size} | Active: {current_db_name}")
            return await ctx.reply(embed=embed)
        except Exception as e:
            return await ctx.reply(f"❌ **Failed to list databases:**\n```\n{e}\n```")

    if parts[0].lower() == "use" and len(parts) >= 2:
        new_db_name = parts[1].strip()
        current_db_name = new_db_name
        return await ctx.reply(f" Switched active default database to **`{current_db_name}`**.")

    if parts[0].lower() in ("collections", "cols"):
        target_name = parts[1].strip() if len(parts) >= 2 else None
        try:
            all_dbs = [d for d in mongo_client.list_database_names() if d not in ("admin", "config", "local")]

            if target_name and target_name.lower() != "all":
                if target_name in all_dbs:
                    target_db = mongo_client[target_name]
                    colls = target_db.list_collection_names()
                    lines = []
                    for col_name in sorted(colls):
                        try:
                            count = target_db[col_name].estimated_document_count()
                            lines.append(f"• **{col_name}**: `{count:,}` docs")
                        except Exception:
                            lines.append(f"• **{col_name}**")

                    embed = discord.Embed(
                        title=f"📁 Collections in `{target_name}`",
                        description="\n".join(lines) if lines else "No collections found.",
                        color=discord.Color.blue()
                    )
                    embed.set_footer(text=f"Total collections: {len(colls)}")
                    return await ctx.reply(embed=embed)
                else:
                    return await ctx.reply(f"❓ Database **`{target_name}`** not found.")

            total_cols_count = 0
            lines = []
            for d in all_dbs:
                c_list = sorted(mongo_client[d].list_collection_names())
                total_cols_count += len(c_list)
                c_formatted = ", ".join(f"`{c}`" for c in c_list) if c_list else "_empty_"
                lines.append(f"• **{d}** ({len(c_list)}): {c_formatted}")

            embed = discord.Embed(
                title=f"📁 All Collections ({total_cols_count} across {len(all_dbs)} databases)",
                description="\n\n".join(lines),
                color=discord.Color.blue()
            )
            embed.set_footer(text="Use `j!db cols <dbname>` to see doc counts for a specific database.")
            return await ctx.reply(embed=embed)
        except Exception as e:
            return await ctx.reply(f"❌ **Failed to list collections:**\n```\n{e}\n```")

    if parts[0].lower() == "stats":
        target = parts[1].strip() if len(parts) >= 2 else None
        try:
            smart_db = SmartDB(mongo_client, active_db_name=current_db_name)
            cmap = smart_db.get_collection_map()
            all_dbs = [d for d in mongo_client.list_database_names() if d not in ("admin", "config", "local")]

            if not target:
                total_collections = sum(len(mongo_client[d].list_collection_names()) for d in all_dbs)
                db_stats_list = []
                total_docs = 0
                total_size = 0

                for d in all_dbs:
                    s = mongo_client[d].command("dbStats")
                    docs = s.get("objects", 0)
                    size = s.get("dataSize", 0)
                    total_docs += docs
                    total_size += size
                    db_stats_list.append(f"• **{d}**: `{docs:,}` docs ({format_size(size)})")

                embed = discord.Embed(
                    title="📊 MongoDB Cluster Overview",
                    color=discord.Color.teal()
                )
                embed.add_field(name="Total Databases", value=f"`{len(all_dbs)}`", inline=True)
                embed.add_field(name="Total Collections", value=f"`{total_collections}`", inline=True)
                embed.add_field(name="Total Documents", value=f"`{total_docs:,}`", inline=True)
                embed.add_field(name="Total Data Size", value=format_size(total_size), inline=True)
                embed.add_field(name="Active DB", value=f"`{current_db_name}`", inline=True)
                embed.add_field(name="Databases Breakdown", value="\n".join(db_stats_list), inline=False)
                return await ctx.reply(embed=embed)

            if target in all_dbs:
                stats = mongo_client[target].command("dbStats")
                embed = discord.Embed(
                    title=f"📊 Stats for Database `{target}`",
                    color=discord.Color.teal()
                )
                embed.add_field(name="Collections", value=f"{stats.get('collections', 0)}", inline=True)
                embed.add_field(name="Total Documents", value=f"{stats.get('objects', 0):,}", inline=True)
                embed.add_field(name="Data Size", value=format_size(stats.get('dataSize', 0)), inline=True)
                embed.add_field(name="Storage Size", value=format_size(stats.get('storageSize', 0)), inline=True)
                embed.add_field(name="Indexes", value=f"{stats.get('indexes', 0)}", inline=True)
                return await ctx.reply(embed=embed)

            if target in cmap:
                col_obj = cmap[target][0]
                stats = col_obj.database.command("collStats", col_obj.name)
                embed = discord.Embed(
                    title=f"📊 Stats for Collection `{col_obj.database.name}.{col_obj.name}`",
                    color=discord.Color.teal()
                )
                embed.add_field(name="Database", value=f"`{col_obj.database.name}`", inline=True)
                embed.add_field(name="Document Count", value=f"{stats.get('count', 0):,}", inline=True)
                embed.add_field(name="Size", value=format_size(stats.get('size', 0)), inline=True)
                embed.add_field(name="Storage Size", value=format_size(stats.get('storageSize', 0)), inline=True)
                embed.add_field(name="Indexes", value=f"{stats.get('nindexes', 0)}", inline=True)
                embed.add_field(name="Avg Doc Size", value=format_size(stats.get('avgObjSize', 0)), inline=True)
                return await ctx.reply(embed=embed)

            return await ctx.reply(f"❓ Could not find database or collection named **`{target}`**.")
        except Exception as e:
            return await ctx.reply(f"❌ **Failed to retrieve stats:**\n```\n{e}\n```")

    env = build_execution_env(ctx)

    try:
        t0 = time.perf_counter()
        result, stdout_str = await execute_pymongo_code(raw_command, env)
        exec_duration = round((time.perf_counter() - t0) * 1000, 2)

        formatted_res = None
        code_lang = "py"
        if isinstance(result, (dict, list)):
            formatted_res = json_util.dumps(result, indent=2)
            code_lang = "json"
        elif hasattr(result, "inserted_id"):
            formatted_res = f"Inserted ID: {result.inserted_id}"
        elif hasattr(result, "inserted_ids"):
            formatted_res = f"Inserted IDs: {result.inserted_ids}"
        elif hasattr(result, "modified_count"):
            formatted_res = f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_id}"
        elif hasattr(result, "deleted_count"):
            formatted_res = f"Deleted count: {result.deleted_count}"
        elif result is not None:
            formatted_res = repr(result)

        stdout_clean = stdout_str.strip()
        response_parts = []
        if stdout_clean:
            response_parts.append(f"**Stdout:**\n```\n{stdout_clean}\n```")
        if formatted_res:
            response_parts.append(f"**Result** (`{exec_duration}ms`):\n```{code_lang}\n{formatted_res}\n```")
        elif not stdout_clean:
            response_parts.append(f"✅ Executed in **{exec_duration}ms** (No output returned).")

        full_response = "\n".join(response_parts)

        if len(full_response) > 1950:
            file_buf = io.BytesIO()
            content_to_save = ""
            if stdout_clean:
                content_to_save += f"=== STDOUT ===\n{stdout_clean}\n\n"
            if formatted_res:
                content_to_save += f"=== RESULT ===\n{formatted_res}\n"
            file_buf.write(content_to_save.encode("utf-8"))
            file_buf.seek(0)

            filename = "result.json" if code_lang == "json" and not stdout_clean else "result.txt"
            discord_file = discord.File(file_buf, filename=filename)
            return await ctx.reply(
                f"⏱️ Finished in **{exec_duration}ms**. Output exceeded Discord limit, attached below:",
                file=discord_file
            )

        return await ctx.reply(full_response)

    except Exception:
        tb = traceback.format_exc()
        if len(tb) > 1900:
            tb = tb[-1900:]
        return await ctx.reply(f"❌ **Execution Error:**\n```py\n{tb}\n```")

if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("BOT_TOKEN not found in environment or .env file.")
    bot.run(token)
