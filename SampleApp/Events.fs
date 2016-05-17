// Events come before the domain objects to avoid the temptation to reuse the domain objects in the events.
// This kind of coupling makes changes to the domain at a later date much more difficult.
namespace SampleApp.Events
type UserInfo = {Name : string; Company : string; Email: string; Password : string}

// Domain events. Past tense.
type UserEvent = 
| UserCreated of UserInfo
| PasswordChanged of newPassword:string