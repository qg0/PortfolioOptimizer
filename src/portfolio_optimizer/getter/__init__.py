"""Load local copy of data and updates it."""

from portfolio_optimizer.getter.legacy_dividends import get_legacy_dividends as legacy_dividends
from portfolio_optimizer.getter.local_cpi import get_cpi as cpi
from portfolio_optimizer.getter.local_dividends import get_dividends as dividends
from portfolio_optimizer.getter.local_index import get_index_history as index_history
from portfolio_optimizer.getter.local_quotes import get_prices_history as prices_history
from portfolio_optimizer.getter.local_quotes import get_volumes_history as volumes_history
from portfolio_optimizer.getter.local_securities_info import get_last_prices as last_prices
from portfolio_optimizer.getter.local_securities_info import get_security_info as security_info
