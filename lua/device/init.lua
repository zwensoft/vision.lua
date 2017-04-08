local conf      = require('ext/conf')
local json      = require('json')
local fs        = require('fs')
local utils     = require('utils')

local exports 	= {}


local deviceInfo


function exports.getRootPath()
    return conf.rootPath
end

function exports.getSystemTarget()
    local platform = os.platform()
    local arch = os.arch()

    if (platform == 'win32') then
        return 'win'

    elseif (platform == 'darwin') then
        return 'macos'
    end

    local filename = exports.getRootPath() .. '/package.json'
    local packageInfo = json.parse(fs.readFileSync(filename)) or {}
    local target = packageInfo.target or 'linux'
    target = target:trim()
    return target
end

function exports.getMacAddress()
    local faces = os.networkInterfaces()
    if (faces == nil) then
    	return
    end

	local list = {}
    for k, v in pairs(faces) do
        if (k == 'lo') then
            goto continue
        end

        for _, item in ipairs(v) do
            if (item.family == 'inet') then
                list[#list + 1] = item
            end
        end

        ::continue::
    end

    if (#list <= 0) then
    	return
    end

    local item = list[1]
    if (not item.mac) then
    	return
    end

    return utils.bin2hex(item.mac)
end

function exports.getDeviceInfo()
    if (deviceInfo) then
        return deviceInfo
    end

    deviceInfo = {}
    local target = exports.getSystemTarget()

    local profile = conf('lpm')
    deviceInfo.model        = profile:get('device.deviceModel') or target
    deviceInfo.type         = profile:get('device.deviceType') or target
    deviceInfo.serialNumber = profile:get('device.deviceId')
    deviceInfo.manufacturer = profile:get('device.manufacturer') or 'CMPP'

    if (not deviceInfo.serialNumber) or (deviceInfo.serialNumber == '') then
    	deviceInfo.serialNumber = exports.getMacAddress() or ''
    end

    deviceInfo.udn          = 'uuid:' .. deviceInfo.serialNumber
    deviceInfo.target       = target
    deviceInfo.version      = process.version

    return deviceInfo
end

return exports
