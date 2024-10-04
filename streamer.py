import asyncio
import websockets
import base64
import os
import json
import pandas as pd
import streamlit as st
import sqlite3
import threading
from queue import Queue
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# ===========================
# Database Initialization
# ===========================

def initialize_db(db_path):
    """
    Initializes the SQLite database and creates the 'ticks' and 'portfolio' tables if they don't exist.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Create 'ticks' table
    c.execute('''CREATE TABLE IF NOT EXISTS ticks (
                    Symbol TEXT PRIMARY KEY,
                    MarketType TEXT,
                    Status TEXT,
                    Timestamp INTEGER,
                    Open REAL,
                    High REAL,
                    Low REAL,
                    Close REAL,
                    Volume INTEGER,
                    BidPrice REAL,
                    BidVolume INTEGER,
                    AskPrice REAL,
                    AskVolume INTEGER,
                    Change REAL,
                    PercentChange REAL,
                    Trades INTEGER
                )''')
    # Create 'portfolio' table
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio (
                    Symbol TEXT PRIMARY KEY
                )''')
    conn.commit()
    conn.close()

# ===========================
# WebSocket Client Class
# ===========================

class WebSocketClient(threading.Thread):
    """
    A threaded WebSocket client that connects to the specified URI,
    receives messages, and puts the data into a queue.
    """
    def __init__(self, uri, headers, data_queue):
        super().__init__()
        self.uri = uri
        self.headers = headers
        self.data_queue = data_queue
        self.stop_event = threading.Event()
        self.daemon = True  # Daemonize thread to exit when main program exits

    def run(self):
        """
        Runs the asyncio event loop for the WebSocket connection.
        """
        asyncio.run(self.connect())

    async def connect(self):
        """
        Establishes the WebSocket connection and listens for incoming messages.
        """
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(self.uri, extra_headers=self.headers) as websocket:
                    # Update connection status in session state
                    st.session_state['ws_connected'] = True
                    st.session_state['ws_error'] = ""
                    while not self.stop_event.is_set():
                        try:
                            message = await websocket.recv()
                            self.handle_message(message)
                        except websockets.exceptions.ConnectionClosed:
                            st.session_state['ws_connected'] = False
                            st.session_state['ws_error'] = "WebSocket connection closed."
                            break
                        except Exception as e:
                            st.session_state['ws_connected'] = False
                            st.session_state['ws_error'] = f"Error receiving message: {e}"
                            break
            except Exception as e:
                st.session_state['ws_connected'] = False
                st.session_state['ws_error'] = f"WebSocket connection error: {e}"
            # Wait before trying to reconnect
            await asyncio.sleep(5)

    def handle_message(self, message):
        """
        Parses the incoming message and puts data into the queue.
        """
        try:
            tick_data = json.loads(message)
            if tick_data.get('t') == 'tick':
                details = tick_data.get('d')
                if details:
                    self.data_queue.put(details)
        except json.JSONDecodeError:
            st.session_state['ws_error'] = "Failed to decode JSON message."

    def stop(self):
        """
        Signals the thread to stop.
        """
        self.stop_event.set()

# ===========================
# Streamlit Application
# ===========================

def upsert_tick_data(conn, details):
    """
    Inserts or updates tick data in the SQLite database.
    """
    try:
        c = conn.cursor()
        c.execute('''INSERT INTO ticks (Symbol, MarketType, Status, Timestamp, Open, High, Low, Close, Volume, 
                                         BidPrice, BidVolume, AskPrice, AskVolume, Change, PercentChange, Trades)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(Symbol) DO UPDATE SET
                        MarketType=excluded.MarketType,
                        Status=excluded.Status,
                        Timestamp=excluded.Timestamp,
                        Open=excluded.Open,
                        High=excluded.High,
                        Low=excluded.Low,
                        Close=excluded.Close,
                        Volume=excluded.Volume,
                        BidPrice=excluded.BidPrice,
                        BidVolume=excluded.BidVolume,
                        AskPrice=excluded.AskPrice,
                        AskVolume=excluded.AskVolume,
                        Change=excluded.Change,
                        PercentChange=excluded.PercentChange,
                        Trades=excluded.Trades
             ''', (
            details['s'], details['m'], details['st'], details['t'], details['o'], details['h'], details['l'], details['c'],
            details['v'], details['bp'], details['bv'], details['ap'], details['av'], details['ch'], details['pch'], details['tr']
        ))
    except Exception as e:
        st.session_state['db_error'] = f"SQLite Error during upsert: {e}"

def highlight_changes(current_df, previous_df):
    """
    Compares current and previous DataFrames and returns a styles DataFrame
    with background colors indicating changes.
    """
    if previous_df.empty:
        # No previous data to compare; return empty styles
        return pd.DataFrame('', index=current_df.index, columns=current_df.columns)
    
    # Ensure 'Symbol' is the index
    current_df = current_df.set_index('Symbol')
    previous_df = previous_df.set_index('Symbol')
    
    # Find common symbols
    common_symbols = current_df.index.intersection(previous_df.index)
    
    if common_symbols.empty:
        # No common symbols to compare
        return pd.DataFrame('', index=current_df.index, columns=current_df.columns)
    
    # Subset the dataframes to common symbols
    current_subset = current_df.loc[common_symbols]
    previous_subset = previous_df.loc[common_symbols]
    
    # Ensure both DataFrames have the same columns
    common_columns = current_subset.columns.intersection(previous_subset.columns)
    if common_columns.empty:
        # No common columns to compare
        return pd.DataFrame('', index=current_subset.index, columns=current_subset.columns)
    
    # Subset the dataframes to common columns
    current_subset = current_subset[common_columns]
    previous_subset = previous_subset[common_columns]
    
    # Compare the DataFrames
    comparison = current_subset > previous_subset
    comparison_lower = current_subset < previous_subset
    
    # Initialize styles DataFrame
    styles = pd.DataFrame('', index=current_subset.index, columns=current_subset.columns)
    
    for col in common_columns:
        styles[col] = [
            'background-color: #d4edda' if inc else
            'background-color: #f8d7da' if dec else ''
            for inc, dec in zip(comparison[col], comparison_lower[col])
        ]
    
    return styles

def main():
    # Streamlit configurations
    st.set_page_config(page_title="Live Stock Ticker Stream", layout="wide")

    # Initialize session state variables
    if 'ws_connected' not in st.session_state:
        st.session_state.ws_connected = False
    if 'ws_error' not in st.session_state:
        st.session_state.ws_error = ""
    if 'db_error' not in st.session_state:
        st.session_state.db_error = ""
    if 'previous_data' not in st.session_state:
        st.session_state.previous_data = pd.DataFrame()
    if 'styles' not in st.session_state:
        st.session_state.styles = pd.DataFrame()

    # CSS for conditional formatting
    st.markdown("""
    <style>
        .cell-up {
            background-color: #d4edda !important;
        }
        .cell-down {
            background-color: #f8d7da !important;
        }
        /* Style for the control buttons */
        .control-button {
            margin-right: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title("Live Stock Ticker Stream")

    # Top Controls: Start, Stop, Refresh, Auto-Refresh Toggle
    with st.container():
        cols = st.columns([1, 1, 1, 2])
        with cols[0]:
            if st.button("Start Stream"):
                if 'ws_client' not in st.session_state:
                    ws_client = WebSocketClient(
                        uri=st.session_state.get('uri', "wss://market.capitalstake.com/stream"),
                        headers=st.session_state.get('headers', {}),
                        data_queue=st.session_state.get('data_queue', Queue())
                    )
                    st.session_state.ws_client = ws_client
                    ws_client.start()
                    st.success("WebSocket client started")
                else:
                    st.warning("WebSocket client is already running")
        with cols[1]:
            if st.button("Stop Stream"):
                if 'ws_client' in st.session_state:
                    st.session_state.ws_client.stop()
                    del st.session_state.ws_client
                    st.session_state.ws_connected = False
                    st.success("WebSocket client stopped")
                else:
                    st.warning("WebSocket client is not running")
        with cols[2]:
            if st.button("Refresh Screen"):
                st.rerun()
        with cols[3]:
            auto_refresh = st.checkbox("Enable Auto-Refresh", value=True)
            refresh_interval = st.slider("Refresh Interval (seconds)", min_value=1, max_value=60, value=2, step=1)

    # Initialize WebSocket URI and headers only once
    if 'uri' not in st.session_state:
        st.session_state.uri = "wss://market.capitalstake.com/stream"

    if 'headers' not in st.session_state:
        def generate_websocket_key():
            random_bytes = os.urandom(16)
            return base64.b64encode(random_bytes).decode('utf-8')
        st.session_state.headers = {
            "accept-language": "en-US,en;q=0.9,ps;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
            "sec-websocket-key": generate_websocket_key(),
            "sec-websocket-version": "13"
        }

    # Initialize a global queue for inter-thread communication
    if 'data_queue' not in st.session_state:
        st.session_state.data_queue = Queue()

    # Database path
    db_path = "tick_data.db"

    # Initialize the SQLite database only once
    if 'db_initialized' not in st.session_state:
        initialize_db(db_path)
        st.session_state.db_initialized = True

    # Function to process data from the queue and write to the database
    def process_queue():
        if 'ws_client' in st.session_state:
            conn = sqlite3.connect(db_path)
            while not st.session_state.data_queue.empty():
                details = st.session_state.data_queue.get()
                upsert_tick_data(conn, details)
            conn.commit()
            conn.close()

    # Function to read and display the latest tick data from SQLite
    def display_data():
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query("SELECT * FROM ticks ORDER BY Timestamp DESC LIMIT 100", conn)
            conn.close()
            if not df.empty:
                # Convert timestamp to readable format
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            st.session_state['db_error'] = f"Error displaying data: {e}"
            return pd.DataFrame()

    # Process the queue and fetch current data
    process_queue()
    current_data = display_data()

    # Retrieve previous data from session state
    previous_data = st.session_state.previous_data

    # Apply highlighting to changes
    try:
        styles = highlight_changes(current_data, previous_data)
        st.session_state.styles = styles  # Store styles in session state
    except Exception as e:
        st.session_state['db_error'] = f"Error during change highlighting: {e}"
        st.session_state.styles = pd.DataFrame('', index=current_data.index, columns=current_data.columns)

    # Update previous data in session state
    st.session_state.previous_data = current_data.copy()

    # Display connection status and errors in the sidebar
    with st.sidebar:
        st.header("Connection Status")
        if st.session_state.ws_connected:
            st.success("WebSocket Connected")
        else:
            st.warning("WebSocket Disconnected")
        if st.session_state.ws_error:
            st.error(f"WebSocket Error: {st.session_state.ws_error}")
        if st.session_state.db_error:
            st.error(f"Database Error: {st.session_state.db_error}")

        st.header("Portfolio Management")

        # Function to fetch all available symbols from 'ticks' table
        def get_all_symbols():
            try:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("SELECT Symbol FROM ticks")
                symbols = [row[0] for row in c.fetchall()]
                conn.close()
                return symbols
            except Exception as e:
                st.error(f"Error fetching symbols: {e}")
                return []

        # Function to fetch current portfolio symbols
        def get_portfolio_symbols():
            try:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("SELECT Symbol FROM portfolio")
                portfolio = [row[0] for row in c.fetchall()]
                conn.close()
                return portfolio
            except Exception as e:
                st.error(f"Error fetching portfolio: {e}")
                return []

        # Function to add symbols to portfolio
        def add_to_portfolio(symbols):
            try:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                for symbol in symbols:
                    c.execute("INSERT OR IGNORE INTO portfolio (Symbol) VALUES (?)", (symbol,))
                conn.commit()
                conn.close()
                st.success(f"Added {len(symbols)} stock(s) to portfolio.")
            except Exception as e:
                st.error(f"Error adding to portfolio: {e}")

        # Function to remove symbols from portfolio
        def remove_from_portfolio(symbols):
            try:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                for symbol in symbols:
                    c.execute("DELETE FROM portfolio WHERE Symbol = ?", (symbol,))
                conn.commit()
                conn.close()
                st.success(f"Removed {len(symbols)} stock(s) from portfolio.")
            except Exception as e:
                st.error(f"Error removing from portfolio: {e}")

        # Display current portfolio
        portfolio_symbols = get_portfolio_symbols()
        if portfolio_symbols:
            st.subheader("Current Portfolio")
            for symbol in portfolio_symbols:
                st.write(f"- {symbol}")
        else:
            st.subheader("Current Portfolio")
            st.write("No stocks in portfolio.")

        # Multi-select to add stocks to portfolio
        all_symbols = get_all_symbols()
        available_to_add = list(set(all_symbols) - set(portfolio_symbols))
        if available_to_add:
            selected_to_add = st.multiselect("Add Stocks to Portfolio", options=available_to_add, default=None)
            if st.button("Add to Portfolio"):
                if selected_to_add:
                    add_to_portfolio(selected_to_add)
                else:
                    st.warning("No stocks selected to add.")
        else:
            st.write("All available stocks are already in your portfolio.")

        # Multi-select to remove stocks from portfolio
        if portfolio_symbols:
            selected_to_remove = st.multiselect("Remove Stocks from Portfolio", options=portfolio_symbols, default=None)
            if st.button("Remove from Portfolio"):
                if selected_to_remove:
                    remove_from_portfolio(selected_to_remove)
                else:
                    st.warning("No stocks selected to remove.")
        else:
            st.write("No stocks to remove from portfolio.")

    # Sidebar Advanced Controls
    st.sidebar.title("Top N Movers Controls")

    top_n = st.sidebar.slider("Select Top N Movers", min_value=1, max_value=50, value=10, step=1)
    top_movers_by = st.sidebar.selectbox(
        "Top Movers Based On",
        options=["Percent Change", "Volume"],
        index=0
    )

    # Apply Filters Based on Portfolio
    if portfolio_symbols:
        display_df = current_data[current_data['Symbol'].isin(portfolio_symbols)]
    else:
        display_df = pd.DataFrame()  # No data to display if portfolio is empty

    # Determine Top Movers
    if top_movers_by == "Percent Change":
        top_movers = display_df.nlargest(top_n, 'PercentChange') if not display_df.empty else pd.DataFrame()
    else:
        top_movers = display_df.nlargest(top_n, 'Volume') if not display_df.empty else pd.DataFrame()

    # Checkbox to Show Top Movers Only
    show_top_movers = st.sidebar.checkbox("Show Top Movers Only", value=False)
    if show_top_movers:
        display_df = top_movers.copy()

    # Highlighting Changes using AgGrid's Conditional Formatting
    if not display_df.empty:
        # Reset index to ensure alignment
        data_to_display = display_df.reset_index(drop=True)
        styles = st.session_state.get('styles', pd.DataFrame()).reset_index(drop=True)

        # Ensure that styles and data_to_display have the same number of rows
        if len(styles) != len(data_to_display):
            st.error("Mismatch between styles and data length. Skipping styling.")
            styles = pd.DataFrame('', index=data_to_display.index, columns=data_to_display.columns)
        else:
            # Add style columns for conditional formatting
            for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'BidPrice', 'BidVolume', 'AskPrice', 'AskVolume', 'Change', 'PercentChange', 'Trades']:
                if col in data_to_display.columns:
                    style_col = f"{col}_Style"
                    data_to_display[style_col] = styles[col]

        # Configure AgGrid options
        gb = GridOptionsBuilder.from_dataframe(data_to_display)
        gb.configure_pagination(paginationAutoPageSize=True)  # Add pagination
        gb.configure_side_bar()  # Add a sidebar
        gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)

        # Apply conditional formatting using cellClassRules
        for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'BidPrice', 'BidVolume', 'AskPrice', 'AskVolume', 'Change', 'PercentChange', 'Trades']:
            if col in data_to_display.columns:
                style_col = f"{col}_Style"
                gb.configure_column(
                    col,
                    cellClassRules={
                        'cell-up': f'data.{style_col} == "background-color: #d4edda"',
                        'cell-down': f'data.{style_col} == "background-color: #f8d7da"',
                    }
                )

        grid_options = gb.build()

        # Remove the style columns before displaying
        for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'BidPrice', 'BidVolume', 'AskPrice', 'AskVolume', 'Change', 'PercentChange', 'Trades']:
            style_col = f"{col}_Style"
            if style_col in data_to_display.columns:
                data_to_display = data_to_display.drop(columns=[style_col])

        # Display using AgGrid
        try:
            AgGrid(
                data_to_display,
                gridOptions=grid_options,
                enable_enterprise_modules=False,
                update_mode=GridUpdateMode.NO_UPDATE,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=True,  # Needed to render CSS classes
                height=600,
                width='100%',
            )
        except Exception as e:
            st.error(f"AgGrid Display Error: {e}")
    else:
        st.write("No data available for the selected filters.")

    # Auto refresh the page based on user selection
    if auto_refresh:
        st_autorefresh(interval=refresh_interval * 1000, limit=None, key="auto_refresh")

    # Cleanup is managed via Stop Stream button; no on_event used

if __name__ == "__main__":
    main()

