local smtp = require('smtp')

local smtp_server = os.getenv("SMTP_SERVER") or "fillme:25"
local smtp_username = os.getenv("SMTP_USERNAME") or "fillme"
local smtp_password = os.getenv("SMTP_PASSWORD") or "fillme"
local smtp_client = nil

function init()
   o = smtp.Options()
   o.Target = smtp_server
   o.Plaintext = true
   o.Username = smtp_username
   o.Password = smtp_password

   smtp_client, err = smtp.New(o)
   if err ~= nil then
      Log:Infof("smtp error %v", err)
      return err
   end

   return nil
end

function tick()
   local err = smtp_client:SendMail("sender@example.com", "receiver@example.com", "hello sub", "how are you")
   if err ~= nil then
      Log:Errorf("smtp error: %v", err)
      return err
   end
   Log:Info("sent mail")
end
