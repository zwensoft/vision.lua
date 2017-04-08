local path 	 = require('path')
local thread = require('thread')
local utils  = require('utils')
local timer  = require('timer')
local json   = require('json')
local sscp   = require('sscp/sscp')


function test_sscp() 
	local sscpClient = sscp.SscpClient:new()
	sscpClient.debugEnabled = true;
	console.log(sscpClient)

	local function onStatus(self, topic, url)
		console.log('onStatus', topic, url)
	end

	sscpClient.status['/status/device'] = onStatus

	sscpClient:onRawMessage("test", "raw data")
	sscpClient:onRawMessage("test", '{"type":"json data"}')
	sscpClient:onRawMessage("test", '{"path":"/status/device", "source":"test"}')
	sscpClient:onRawMessage("test", '{"path":"/notify/device", "source":"test"}')
	sscpClient:onRawMessage("test", '{"path":"/control/device", "source":"test"}')
	
end

test_sscp()
