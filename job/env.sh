#!/usr/bin/env bash
export QUERY_MYSQL_HANDLER_HOST=${QUERY_MYSQL_HANDLER_HOST:="127.0.0.1"}
export QUERY_HTTP_HANDLER_PORT=${QUERY_HTTP_HANDLER_PORT:="8000"}
export BENDSQL_CLIENT_CONNECT="bendsql -uroot --host ${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never -D imdb"

query() {
	echo ">>>> $1"
	echo "$1" | $BENDSQL_CLIENT_CONNECT
	echo "<<<<"
}

stmt() {
	echo ">>>> $1"
	echo "$1" | $BENDSQL_CLIENT_CONNECT
	if [ $? -ne 0 ]; then
		echo "<<<<"
	fi
	return 0
}
