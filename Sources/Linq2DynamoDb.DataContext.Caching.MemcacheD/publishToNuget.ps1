param([string] $apiKey = $(throw 'API key must be specified'),
[string] $version = $(throw 'Version must be specified'),
[string] $nugetPath = '..\nuget.exe',
[string] $outDir = 'bin\Release'
)

$specFile = 'Linq2DynamoDb.DataContext.Caching.MemcacheD.nuspec'
$packageName = 'Linq2DynamoDb.DataContext.Caching.MemcacheD'

function InvokeNuget([string] $command)
{
	Invoke-Expression "& '$nugetPath' $command"
}

InvokeNuget "SetApiKey $apiKey"
InvokeNuget "pack $specFile -OutputDirectory $outDir -Prop Configuration=Release -Version $version"
InvokeNuget "push $outDir\$packageName.$version.nupkg"