from mako.template import Template

pyload_queries = Template('''
require("run_common")

function event()
% for trx_number in transaction_number:
        if not sysbench.opt.skip_trx then
            con:query("BEGIN")
        end

        % for trx_execute in transaction_number[trx_number]:
           ${trx_execute}()
        % endfor

        if not sysbench.opt.skip_trx then
            con:query("COMMIT")
        end
% endfor

% for e_queries in query_execute:
    ${e_queries}
% endfor

end



''')

oltp_common = Template('''
require("run_variables")

function interp(s, tab)
  return (s:gsub('($%b<>)', function(w) return tab[w:sub(3, -2)] or w end))
end

getmetatable("").__mod = interp

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: run")
end

sysbench.cmdline.options = {
    point_selects = {"Number of point SELECT queries to run", 5},
    skip_trx = {"Do not use BEGIN/COMMIT; Use global auto_commit value", false},
% for cmd_queries in cmdline_queries:
     q${cmd_queries} = {"q${cmd_queries}", 1},
    % endfor
}




% for q in cmdline_queries:
function execute_q${q}()
   for i = 1, sysbench.opt.q${q} do
            <%b =sysb_trx.get(q) %>
            con:query(${b})
   end
end
% endfor

function execute_selects()
    -- loop for however many the user wants to execute
    for i = 1, sysbench.opt.point_selects do
        con:query(string.format(randQuery, id))
    end
end


-- Called by sysbench to initialize script
function thread_init()

    -- globals for script
    drv = sysbench.sql.driver()
    con = drv:connect()
end


-- Called by sysbench when tests are done
function thread_done()

    con:disconnect()
end

''')

pyload_variables = Template("""
function get_random(var)
    i = math.random(1, #var )
    return var[i]
end

% for seq in queries:
% for var in values_list[seq]:
<% b = values[seq].get(f"v{var}") %>
q${seq}_v${var}=${b}
% endfor
% endfor
    """)
