package dev.turnstile;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;

/**
 * The domain earns its keep by being testable without a container, a broker, or
 * a Spring context. That property is easy to lose one convenient import at a
 * time, so it is enforced rather than documented.
 */
@AnalyzeClasses(
    packages = "dev.turnstile",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  @ArchTest
  static final ArchRule domain_is_framework_free =
      ArchRuleDefinition.noClasses()
          .that()
          .resideInAPackage("..domain..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework..",
              "jakarta.persistence..",
              "org.apache.kafka..",
              "com.fasterxml.jackson..")
          .because(
              "the seat state machine must stay runnable in a plain unit test; "
                  + "persistence and messaging belong in the layers around it");

  @ArchTest
  static final ArchRule event_store_does_not_depend_on_command_layer =
      ArchRuleDefinition.noClasses()
          .that()
          .resideInAPackage("..eventstore..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("..command..")
          .because("the log is a lower layer than the handler that writes to it");

  @ArchTest
  static final ArchRule domain_does_not_depend_on_the_event_store =
      ArchRuleDefinition.noClasses()
          .that()
          .resideInAPackage("..domain..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("..eventstore..")
          .because(
              "the aggregate decides, it does not persist; keeping it ignorant of "
                  + "the store is what makes it a pure function of its history");
}
