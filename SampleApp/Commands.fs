namespace SampleApp.Commands
  
  type SignUpData = {Name: string; Company : string; Email: string; Password: string}

  // The verbs (actions) of the system (in imperative mood / present tense)
  type Command = 
  | SignUp of SignUpData