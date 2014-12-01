worker_processes  2;

error_log logs/error.log error;
pid logs/nginx.pid;
user root;

events {
	use epoll;
	worker_connections 2048;
}
http {
	default_type application/json;

	log_format main '[$time_local] $remote_addr $request_uri '
					'[$http_user_agent] $bytes_sent $request_time '
					'"$request_body" $host $status';
	access_log logs/access.log main;

	keepalive_timeout 120;
	upstream fmserver{
		server 192.168.1.120:8080 weight=2;
		server 192.168.1.120:8090 weight=2;
	}
	server {
		root /home/lingban/analze/nginx_analyze;
		listen 8080;
        location /query_top{
			content_by_lua_file "nginx_analyze/service.lua";
		}
    }
	server {
		root /home/lingban/analze/nginx_analyze;
		listen 8090;
		location /alarm{
			content_by_lua_file "nginx_analyze/alarm.lua";
		}
		location /operate{
			content_by_lua_file "nginx_analyze/operate.lua";
		}
	}
}
