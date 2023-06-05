<?php

namespace CodeGenerator\Command;


use CodeGenerator\CodeGenerator;
use OpenAPI\Parser;
use Psr\Log\NullLogger;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class GenerateCommand extends Command
{
    protected function configure()
    {
        $this->setName('generate')
            ->setDescription('Generates code from swagger');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $Parser  = new Parser();
        $Swagger = $Parser->parse(__DIR__ . '/../../openapi/swagger.json');

        $CodeGenerator = new CodeGenerator($Swagger, new NullLogger());

        $CodeGenerator->generateDefinitions($Swagger->definitions->getPatternedFields());

        $CodeGenerator->generateApis($Swagger->paths->getPatternedFields());

        $CodeGenerator->generateResponseTypes($Swagger->paths->getPatternedFields());

        return Command::SUCCESS;
    }

}
