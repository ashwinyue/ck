package service

import "github.com/google/wire"

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewGreeterService, NewCronService, NewETLExecutor, NewCronJobManager, NewETLAPIService, NewETLService)
